# Constraints

Hard and soft constraints for **FF Eth Logs**. Treat these as guardrails when changing behaviour, schema, or operations. API rules live in [`docs/api_design.md`](api_design.md); intent in [`docs/business_requirements.md`](business_requirements.md).

## 1. Data and consistency constraints

- **Coverage is an interval, not a head** — `ingest_cursor` holds `[coverage_start, block_number]`, the contiguous run every block and warehouse log of which is stored; the API refuses any range outside it (`out of warehouse scope: … extend below the warehouse coverage start N`), so a fresh database that starts tail ingestion at the tip never answers genesis..head with `[]`. `WriteRange` refuses a range that is not contiguous with the interval (`ErrCoverageGap`), so a `start_block` that jumps ahead cannot leave a hole; `rewind` refuses to go below the start.
- **Logs and block metadata come from one chain** — block metadata is read before the logs; a batch is written only when every raw log's `blockHash` equals the block row for its height and every block that returned no raw log still carries that hash when re-read after the log fetch; ingestion refetches on disagreement and `WriteRange` refuses a mismatched pairing outright. A log with no `blockHash` is unverifiable: ingestion treats it as a disagreement (refetch, then fail) and `WriteRange` rejects it — nothing is stored on trust.
- **Readers are excluded during a backfill, durably** — a backfill holds the maintenance lock exclusively for its whole run and every API read snapshot takes it shared, and before it mutates a unit inside the published coverage it sets `warehouse_state.maintenance`, which reads also check; the flag is cleared only by a verified `finish`. So a reader (including an API-only replica that holds no writer lock) is refused with a scope error while partitions are being reloaded under a still-published coverage — even after a reload died between partitions — instead of being served a mix of old and new partitions.
- **Cursor never ahead of data** — `ingest_cursor.block_number` is the warehouse head and the upper bound every served range is checked against. It moves only inside the transaction that writes the blocks and logs it accounts for (`logstore.WriteRange`); the backfill sets it last (`finish`), and only after the database, the Parquet footers and the local files all match `manifest.json`, which is generated from the export's source (BigQuery counts, GCS checksums) and never from the copy. A crash can leave the cursor behind the data (the next batch re-fetches and overwrites), never ahead of it.
- **Confirmation lag is the reorg strategy** — a block is written only once the tip is `ethereum.confirmation_blocks` (default 2) above it. Heads that change below that depth are absorbed in memory (`Ethereum shallow reorg absorbed within confirmation lag`, info).
- **Deep reorgs rewind to a verified ancestor** — a replaced written height triggers a walk down the persisted `eth_blocks` hashes against canonical headers until they agree; everything above that verified common ancestor is deleted (`logstore.Rewind`) and re-fetched, and the event is logged at error level (reaches Sentry). The target is never derived from the first retained mismatch, because only the last written head is retained and the real fork can sit lower. A fork deeper than 1,024 blocks or one reaching below the covered interval is fatal instead of guessed ([operations](operations.md)).
- **Shape rules are identical in both write paths** — `eventset.Keep` (tail ingestion) and the BigQuery extract `WHERE` clause (backfill, [probe](probe_2026-08.md)) must stay in lock-step. Any drift changes served results depending on how a block reached the warehouse.
- **Every stored height has an `eth_blocks` row** — the reader joins `eth_blocks` for `blockHash` / `blockTimestamp`; `WriteRange` refuses a batch whose block rows do not cover `[from, to]`, because a missing row would silently drop that block's logs from every response.
- **Replays are idempotent** — `WriteRange` deletes the range before COPY; re-ingesting a range after a restart overwrites rather than duplicates.
- **Migrations** — `db/migrations/NNN.sql`, mirrored into `db/init_pg_db.sql`; the four secondary index statements must be byte-identical to `logstore.SecondaryIndexes`.

## 2. API and compatibility constraints

- **Wire shapes are go-ethereum v1.16.5's** — the server is `rpc.Server` from go-ethereum, the log JSON is `types.Log`, the filter decoder is a copy of `filters.FilterCriteria.UnmarshalJSON`. Framing, batching, `-32601` / `-32602` / `-32700` codes and messages are geth's, not an imitation.
- **Error strings verbatim** — `invalid topic(s)`, `invalid block range params`, `unknown block`, `pending logs are not supported`, `exceed max topics`, `cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other` are copied from `eth/filters/api.go` so a client sees what a node would say.
- **Scope errors are `-32000`** and their message deliberately avoids the words a range-cap classifier keys on (`range`, `limit`, `too many`): `out of warehouse scope: <reason>`. A client must treat it as "not answerable here", never as a window to halve.
- **The result cap mimics Infura** — exceeding `rpc.max_results` returns `query returned more than N results`, the phrasing the indexer's pagination already classifies as too-many-results, so the client halves its window instead of failing. It is a plain `-32000` handler error.
- **`eth_blockNumber` is the head, not the tip** — clients that need the chain tip ask the chain. Changing this would break the split point of every routing client.
- **Event set changes are contract changes** — adding a signature or a shape to `internal/eventset` requires re-exporting history, updating [`api_design.md`](api_design.md) and [`schema.md`](schema.md), and keeping `IsWarehouseSignature` (serving) and `Keep` (ingestion) consistent.

## 3. Performance and scale constraints

- **No span cap, no pagination** — a genesis-to-head filter is one SQL query. The only cap is `rpc.max_results` (default 100,000; `0` disables it, not recommended: a genesis-wide `Transfer` query is 293 M rows) and `rpc.query_timeout` (60 s) per query.
- **Every query must be index-served** — the reader always has a block range (`PRIMARY KEY (block_number, log_index)` scan on a partition) and usually a selective column: `topic1` / `topic2` / `topic3` (owner scans) or `address` (contract provenance). There is no index on `topic0` alone or on `tx_hash`; a filter that only pins `topic0` over a wide range is a partition scan and is bounded by `max_results`.
- **Partitions per 1,000,000 blocks** — 40 created up front (to block 40 M, ~2031); `WriteRange` probes the catalog and creates a missing partition inside the write transaction (`TestWriteRangeCreatesPartitionOnRollover`). Partition boundaries match the export directories so the backfill loads one directory per partition.
- **Bulk load drops the indexes** — loading 400 M rows through four random-order btrees would take days; `backfill` drops them (`prepare`), loads sorted, and recreates (`finish`). Sorting happens in PostgreSQL (`work_mem`-bounded external sort) because the busiest partition holds ~60 M rows.
- **Response size** — `server.write_timeout` (120 s) is sized for encoding a 100k-log response.

## 4. Operational constraints

- **No provider call is unbounded** — every RPC attempt runs under `ethereum.rpc_timeout` (60 s); a wedged call after a head was delivered (when the newHeads watchdog is stopped) is retried within the 5-minute budget and then fails ingestion, so a stalled provider cannot leave the head frozen behind a green `/health`.
- **Silence is a failure** — a `newHeads` subscription that delivers nothing for `ethereum.head_timeout` (5 m; a block is due every 12 s) ends the process with `newHeads subscription is silent`, because a half-open socket produces neither heads nor an error and `/health` would keep answering 200 from the database. The watchdog is armed only while waiting for a head, so a long catch-up does not trip it.
- **Mainnet only, enforced** — `ethereum.chain_id` must be `1` (`config.Validate`: the schema carries no chain identity, so another chain's data could later be served as mainnet), and ingestion calls `eth_chainId` before reading the cursor and refuses to start unless the provider reports it (`provider chain id X does not match configured ethereum.chain_id Y`).
- **Single process** — `serve` runs the API and tail ingestion together; either failing stops the other (an API alone would serve a head that silently stops moving). An API-only replica is possible with `ethereum.ingestion_enabled=false` but shares the same database.
- **Ingestion failures are fatal** — subscription errors, fetch failures after the retry budget (5 s → 30 s backoff, 5 min total), sink failures and a too-large catch-up all end the process. There is no in-process reconnect; the supervisor restarts it and it resumes from the cursor.
- **`ethereum.max_catchup_blocks`** (default 50,000 ≈ 7 days) bounds the cursor-to-tip gap on start, measured to the tip including the confirmation window. A larger gap is `ingestion catch-up exceeds max_catchup_blocks`, a startup failure; raise the knob deliberately. `confirmation_blocks` must be below it.
- **Backfill runs with ingestion stopped** on an empty warehouse — `backfill` drops the secondary indexes and, at `finish`, publishes the manifest's interval as coverage after verifying the database against it; a concurrent tail writer would race both.
- **Disk** — ≈ 205 GB modelled today plus ≈ 1.5 GB/month; provision ≈ 250 GB and watch growth. The export copy (≈ 16 GB) is only needed during the backfill.
- **Vendor budget** — the tail shares the indexer's Chainstack endpoint. Steady state is one `newHeads` push and one 1-block `eth_getLogs` per block, plus `eth_getBlockByNumber` for boundary heads after a resubscribe and `eth_getBlockReceipts` for a dense block. A catch-up walks 10-block batches; the ingestion filter is topic0-only and returns ~470 raw logs per block (mostly ERC-20), which is why batches are small.
- **Configuration** — YAML plus `FF_ETH_LOGS_*` overrides; secrets (the WebSocket URL with its key, the database password) live in `config/.env.local` or the deployment's secret store, never in the repository.
- **Health is liveness of the database path** — `GET /health` is 200 whenever the database answers, including during a long catch-up. Head lag is for dashboards, not for the healthcheck.

## 5. Security constraints

- **No authentication** — the endpoint has no auth, no TLS, no rate limiting. It must bind on the private backend network only; `server.host` defaults to `0.0.0.0` inside the container and the deployment must not publish the port beyond that network.
- **RPC URL carries the key** — `ethereum.websocket_url` includes the provider API key in the path. `chain.endpointForLogs` reduces it to scheme and host in log fields, and `chain.RedactURLs` strips the path from every URL inside an error value before it is logged, returned or forwarded to Sentry (transport errors quote the full URL); do not log the URL elsewhere.
- **Query parameters travel as bind parameters** — `logstore.buildFilter` assembles SQL from fixed fragments; addresses and topics are `bytea[]` parameters, never interpolated.
- **Sentry** receives error-level events only (deep reorgs, fatal exits) with info lines as breadcrumbs; log lines carry block numbers and hashes, not request bodies.

## 6. Deployment constraints

- **Image plus a mounted config** — the container runs the image with `/app/config.yaml` mounted; environment variables layer on top. env files, local SQL dumps and docs are excluded from the image (`.dockerignore`); the tracked schema (`db/init_pg_db.sql`, `db/migrations/`) ships at `/app/db` so the deployment applies exactly the schema the pinned image was built against (`psql -f`, idempotent, every deploy).
- **Own PostgreSQL container and volume** via ff-deploy — the warehouse is 10–50× the indexer database, is fully reproducible from GCS, and its vacuum/bloat profile must not touch the indexer's `jobs` queue; it is excluded from backups.
- **PostgreSQL 18, Go 1.25** are the supported baselines (CI runs the integration suite against `postgres:18`).
- **Schema before code** — run a migration before deploying the binary that depends on it.

## 7. Non-goals

- **Being a node** — no state, no transactions, no receipts, no tip; the vendor keeps those.
- **Serving ERC-20 or arbitrary events** — the stored set is the NFT set the indexer consumes; broadening it is a re-export, not a config change.
- **Reorg repair below coverage** — a fork that reaches below `coverage_start` is not repaired automatically; rebuild from the export.
- **Multi-tenant or public exposure** — one internal consumer on a private network.
- **Analytics** — point and range lookups only; no aggregation surface.
