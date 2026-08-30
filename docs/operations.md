# Operations

Runbook for **FF Eth Logs**: first deployment, the backfill, reorg and catch-up procedures, monitoring, and deployment through ff-deploy. Commands assume the binary on the host or a one-off container (`docker compose run --rm ff-eth-logs …`); every subcommand takes `-config <file>` and `-env <dir>`. `backfill` and `serve` exclude each other through a database writer lock: a backfill stage against a running service fails with `another writer holds the warehouse` — stop the service first.

## 1. First deployment

### 1.1 Create the database

Apply the schema to the database the service will use:

```bash
psql -h <db-host> -U postgres -d ff_eth_logs -f db/init_pg_db.sql
```

The script is idempotent (`IF NOT EXISTS` throughout). ff-deploy does this on **every** deploy: the image ships `db/` at `/app/db`, and the role extracts `/app/db/init_pg_db.sql` from the pinned image and applies it with `psql -v ON_ERROR_STOP=1` before the service container is (re)created, so the database always matches the binary about to run. Provision **≈ 250 GB** of disk: the load ends at ≈ 205 GB and grows ≈ 1.5 GB/month.

### 1.2 Copy the export

The export lives in GCS (US multi-region, Parquet/SNAPPY). As of the run of 2026-08-28 the `v1` prefix holds the warehouse set to block **25,842,829** (dataset head, 2026-08-26 23:59:59 UTC):

| Prefix | Size | Files |
| --- | --- | --- |
| `gs://eth-logs/eth-log-warehouse/v1/logs/part=000` … `part=025` | 14.87 GB | 1,581 (`logs-*.parquet`) |
| `gs://eth-logs/eth-log-warehouse/v1/blocks` | 1.25 GB | 4,048 (`blocks-*.parquet`) |

```bash
gcloud storage cp -r gs://eth-logs/eth-log-warehouse/v1 ./data/
```

The copy (≈ 16 GB) is only needed for the load; it is the disaster-recovery source and stays in GCS.

**The manifest.** `finish` requires `manifest.json` at the export root and refuses to publish coverage without it. It is generated from the export's source, not from the downloaded files, so that a partial or corrupted copy can never match it:

```json
{
  "export": "eth-log-warehouse/v1",
  "source": "bigquery-public-data.crypto_ethereum, extract of 2026-08-28",
  "blocks": {"first": 0, "last": 25842829, "rows": 25842830},
  "logs":   {"rows": 402266375, "parts": {"000": 0, "001": 0, "...": 0, "025": 123456}},
  "files":  {"logs/part=016/logs-000000000000.parquet": {"size": 11543755, "md5": "<base64>"}, "blocks/blocks-000000000000.parquet": {"size": 300706, "md5": "<base64>"}}
}
```

- `blocks` and `logs` come from BigQuery, against the materialized extract table (cents to run):
  ```sql
  SELECT DIV(block_number, 1000000) AS part, COUNT(*) AS rows FROM `indexer-eth-logs.eth_warehouse.logs` GROUP BY part ORDER BY part;
  SELECT COUNT(*) AS rows FROM `indexer-eth-logs.eth_warehouse.logs`;
  SELECT MIN(number) AS first, MAX(number) AS last, COUNT(*) AS rows FROM `bigquery-public-data.crypto_ethereum.blocks` WHERE number <= 25842829;
  ```
  `blocks.last` is the export's **`export_end`** — the single block captured at the start of the extract script that bounds the logs materialization and the blocks export alike (see [docs/probe_2026-08.md](probe_2026-08.md)); for `v1` it is 25,842,829, the dataset head at materialization. Never derive it from the blocks export itself: the live blocks table can advance between the two steps, and `backfill blocks` trims what it loads to the manifest interval precisely so that a longer blocks export cannot widen coverage past the logs extract. Every partition from `first/1000000` to `last/1000000` must have an entry, zero for the empty ones.
- `files` comes from the objects as GCS stores them: `gcloud storage ls --format=json -r 'gs://eth-logs/eth-log-warehouse/v1/**'` yields `size` and `md5_hash` (base64) per object; the key is the object path relative to the export root.

Assemble the three into `manifest.json`, upload it next to the export so every copy carries it, and never edit it on the loading host. **`v1` has its manifest**: `gs://eth-logs/eth-log-warehouse/v1/manifest.json` (uploaded 2026-08-30; 26 partitions `000`–`025`, 402,266,375 logs, blocks 0–25,842,829, 5,629 files / 16.12 GB), so `gcloud storage cp -r gs://eth-logs/eth-log-warehouse/v1 ./data/` brings it along. The `v0-rehearsal` prefix deliberately has no manifest.


### 1.3 Run the backfill

The backfill goes into an **empty** warehouse, before the service ever ingests. A database that has already followed the chain head (a tail-only start) publishes coverage far above the export's end, and `finish` could never merge the two intervals — so every stage refuses it up front (`the export covers blocks A-B but the warehouse already publishes X-Y (a tail-only start); recreate the database …`). Recreate the database and load first.

With the service **not running** (or `ethereum.ingestion_enabled=false`) on the empty warehouse:

```bash
ff-eth-logs backfill -config /app/config.yaml -dir ./data/v1
```

Stages run in order and are each idempotent (`-stage prepare|logs|blocks|finish` runs one):

1. `prepare` — drops the four secondary indexes (`Dropped index for bulk load`).
2. `logs` — one transaction per `part=NNN` directory: COPY into a temp staging table, `INSERT … ORDER BY (block_number, log_index)` into the partition (`Partition loaded` with `rows` and `took`; on re-run `Partition already loaded from this export, skipping` when the row count and the recorded manifest fingerprint both match, otherwise the partition is cleared and reloaded). The busiest partition holds ~60 M rows and is sorted by PostgreSQL.
3. `blocks` — all 4,048 files into `eth_blocks` in one transaction (`Blocks load progress` every 500 files), keeping only rows inside the manifest's `[first, last]` (requires `manifest.json`; see 1.2 on why the blocks export can be longer than the logs extract).
4. `finish` — verifies the load against `manifest.json` at the export root (section 1.2): `eth_blocks` must be exactly the manifest's interval and row count; every partition the interval implies must exist and hold the manifest's row count both in its Parquet footers and in the database; every listed file must match its recorded size and MD5 and no unlisted Parquet file may be present. Any mismatch is `backfill is not complete, cursor not set: …` and nothing is published. Then it recreates the indexes (`Index ready` per index with `took`), `ANALYZE`, and publishes coverage `[manifest first, manifest last]` (`Backfill finished; coverage published`). Until `finish` succeeds the API reports the warehouse as empty.

**Durations are not yet measured** — record the `took` fields of the first production run here. Session-level PostgreSQL settings that matter: `maintenance_work_mem` for the four index builds over 400 M rows (e.g. `maintenance_work_mem = '2GB'`), `work_mem` for the per-partition sort (anything below the partition size spills to an external sort, which is correct but slower), and enough WAL headroom (`max_wal_size`) for a multi-GB COPY per transaction. Set them in `postgresql.conf` for the load or `ALTER SYSTEM`, and reset afterwards.

A failed stage rolls its transaction back; re-run the same command and it resumes at the first unloaded partition.

### 1.4 Start serving and let the tail catch up

Start `serve`. Ingestion resumes at cursor + 1 = **25,842,830** and logs `Ethereum ingestion catching up to head` with `blocks` = the gap. The gap is bounded by `ethereum.max_catchup_blocks` (default 50,000 ≈ 7 days at ~7,200 blocks/day): **if the export is older than that at first start, raise `ethereum.max_catchup_blocks` above the gap for the first run** (the process otherwise exits with `ingestion catch-up exceeds max_catchup_blocks`), then set it back. `ethereum.confirmation_blocks` must stay below it. The catch-up walks 10-block batches, committing each; the plan estimated ~80 min for a 14k-block gap — measure and record it.

### 1.5 Verify

```bash
curl -s http://<host>:8545/health
# {"status":"ok","head":<block>,"coverage_start":0,"empty":false,"chain_id":1}
```

`head` must be within a few blocks of the chain tip once the catch-up finishes. Then send the same `eth_getLogs` body to the warehouse and to the vendor for an in-scope filter at or below the head — for example a CryptoPunks `PunkTransfer` query on `0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb` over a closed range — and diff the arrays; they must be identical.

## 2. Deep reorg

**Signal** — an error-level line (reaches Sentry):

```text
ethereum reorg deeper than confirmation lag: rewinding to the verified common ancestor
  replacedHeight=<h> lastWritten=<n> ancestor=<a> blocksDropped=<n-a>
```

A written block was replaced by the chain. Recovery is automatic and happens before anything else is written: ingestion walks down from the replaced height comparing each canonical header (`eth_getBlockByNumber`) with the hash persisted in `eth_blocks` until they agree — that block is the verified common ancestor — then deletes every block above it (`logstore.Rewind`), restarts the stream at `ancestor+1`, and re-fetches the canonical blocks through the confirmed head. Nothing stale stays inside the advertised coverage, and the operator target is never derived from the first retained mismatch (the last written head is the only retained one, so the real fork can sit lower).

Only the log line needs attention: check that the re-fetch completed (`/health` `head` moving again) and, if the reorg was unusually deep, that the provider is healthy.

**When recovery is fatal** — the process exits with one of:

- `reorg deeper than confirmation lag replaced written block <h>: no common ancestor within 1024 blocks below <h>; verify the chain and rewind manually`
- `… block <n> is not stored, the fork reaches below warehouse coverage; rebuild from the export`
- `… rewind to <a> after deep reorg: … below the coverage start …`

These mean the fork is deeper than any mainnet reorg should be, or reaches below what the warehouse holds. The supervisor will restart into the same detection, so decide first: verify the chain against a second provider, then either `ff-eth-logs rewind -config /app/config.yaml -to <verified ancestor>` (with the service stopped; `-to` is the last block to keep, and it must be at or above `coverage_start`) or rebuild from the export (section 5).

**Manual rewind** — `ff-eth-logs rewind -to N` is also the tool for re-ingesting a range that is suspected stale for any other reason (a provider incident, a deliberate re-check). Stop the service, rewind, start; it resumes at `N+1`. `rewind` takes the same writer lock as ingestion and the backfill, so against a running service or backfill it fails with `another writer holds the warehouse` instead of deleting under them. `-to 0` is valid on a full-history warehouse (keeps genesis, re-ingests from block 1); the flag must be present — its absence, not its value, is the error.

A deep reorg that spans a process restart is detected at start: before the first head, ingestion compares the persisted hash of the cursor block with the canonical header and runs the same recovery on a mismatch (`Written head is no longer canonical at start; a reorg happened while the process was down` at warn, then the error line above).

## 3. Catch-up too large

**Signal** — the process exits at start with:

```text
ingestion catch-up exceeds max_catchup_blocks: need blocks A-B (N blocks, max M); rewind deliberately or raise ethereum.max_catchup_blocks
```

The gap from the cursor to the tip exceeds the bound; the bound exists so a stale database or an unreviewed rewind does not start a long unattended RPC walk. Raise `ethereum.max_catchup_blocks` above `N`, restart, and lower it again once `/health` shows the head near the tip. The supervisor will otherwise restart the process into the same error.

## 4. Re-running the backfill

The stages are idempotent, so the same command resumes after a failure. Each stage verifies its files against `manifest.json` before reading a row and records, per unit (`logs/part=NNN`, `blocks`), the fingerprint of the manifest entries it loaded from (`backfill_units`). To load a corrected export: copy the new files and their new manifest over the old ones and rerun `-stage logs` / `-stage blocks` — every unit whose fingerprint changed is cleared and reloaded, unchanged units are skipped, and `finish` refuses to publish while any unit in the interval was loaded from a different manifest than the one present (`… was loaded from a different export than manifest.json describes … rerun its stage`). No manual `TRUNCATE` is needed. A corrected reload also sets the durable `warehouse_state.maintenance` flag before it touches published coverage: if the run is interrupted between partitions, reads keep returning the maintenance scope error (and `/health` `503 {"status":"maintenance"}`) with no process running, until the remaining stages and `finish` complete — `finish` clears the flag after verifying every unit.

`finish` trusts only `manifest.json`, which is written from the export's *source* (BigQuery row counts, GCS checksums), never from the copy: a partial export (the one-day `v0-rehearsal` prefix has no manifest and is refused) or a truncated copy cannot match it. Only an export with a manifest — `v1`, or a full re-export with a freshly generated manifest — is a valid input.

Never run `backfill` while ingestion is writing: `finish` verifies the database against the manifest and publishes the manifest's interval, which a concurrent writer would invalidate mid-check, and `prepare` would drop the indexes the API is serving from.

On start, ingestion verifies the provider with `eth_chainId`; a mismatch (`provider chain id X does not match configured ethereum.chain_id Y`) is fatal before any block is written. A non-zero `ethereum.start_block` is refused at start once a cursor exists (`ethereum.start_block=N is set but the warehouse already has a cursor at C; unset it … or … run \`ff-eth-logs rewind -to N-1\``): a persistent setting would bypass the durable cursor on every restart. To re-ingest from a height deliberately: stop the service, `rewind -to <height-1>`, keep `start_block` at 0, start.

## 5. Rebuilding from scratch

1. Stop the service; drop and recreate the database; apply `db/init_pg_db.sql`.
2. Fresh copy from `gs://eth-logs/eth-log-warehouse/v1` (or a newer prefix from a re-export, section 9).
3. `backfill -dir …`, then `serve` with `ethereum.max_catchup_blocks` sized for the export's age (section 1.4).

The indexer keeps working during the rebuild only if its routing client falls back to the vendor on `out of warehouse scope: warehouse is empty` and on a head far behind the tip; plan the rebuild accordingly.

## 6. Monitoring

**Head lag** — poll `GET /health` and compare `head` with the chain tip (`eth_blockNumber` on the vendor). Steady state: tip − head ≈ `confirmation_blocks` + 1. A head that stops moving with the process still up should not happen (ingestion failure ends the process); a process that keeps restarting is the signal to read the logs.

**Log lines** (JSON, field `component`):

| Level | Message | Meaning |
| --- | --- | --- |
| info | `Starting ethereum ingestion` | `fromBlock`, `confirmationBlocks`, `maxCatchupBlocks` |
| warn | `No cursor and no start_block: starting at the chain head; history below it needs a backfill` | Empty database started without a backfill |
| info | `Ethereum ingestion catching up to head` | `fromBlock`, `toBlock`, `blocks` — restart or socket gap being filled |
| info | `Ethereum ingestion catch-up progress` | every 50 batches: `throughBlock`, `targetBlock`, `batches` |
| info | `Dense block served from receipts (eth_getLogs result cap)` | `block`, `receiptLogs`, `matched` — a block over the provider's result cap; normal, occasional |
| info | `Ethereum shallow reorg absorbed within confirmation lag` | `height`, `old`, `new` — absorbed, no action |
| **error** | `ethereum reorg deeper than confirmation lag: a written block was replaced` | section 2 |
| warn | `retryable ethereum error encountered` | `operation`, `url` (scheme + host) — provider trouble under retry |
| error (exit) | `newHeads subscription is silent: no head in 5m0s (last written block N); restarting to reconnect` | The socket went half-open; the supervisor restart reconnects. Repeated occurrences mean the provider's WebSocket is unreliable |
| info | `Log pagination walk progress` | every 250 windows of a long walk |
| **error** | `ff-eth-logs stopped with error` | The exit line; the wrapped error says why (`new heads subscription error`, `fetch ingestion logs for blocks A-B`, `write blocks A-B`, catch-up bound) |
| info | `JSON-RPC server listening` | `addr` |
| info | `Tail ingestion disabled; serving the stored head only` | `ingestion_enabled=false` |

**Sentry** — `sentry_dsn` sends error-level events only (the two error lines above and the exit), with info lines as breadcrumbs; the service is tagged `service=ff-eth-logs`.

**Database** — `SELECT block_number, updated_at FROM ingest_cursor;` shows the head and when it last moved; `pg_total_relation_size('eth_logs')` tracks growth against the ≈ 1.5 GB/month model.

## 7. Deployment via ff-deploy

**PostgreSQL needs `shm_size`.** Parallel query workers pass data through `/dev/shm`; with Docker's default 64 MB a broad `eth_getLogs` (tens of thousands of matches) fails with `could not resize shared memory segment … No space left on device` (SQLSTATE 53100). Set `shm_size: "1g"` on the PostgreSQL service in the compose file (the dev compose does), or set `dynamic_shared_memory_type = mmap` / `max_parallel_workers_per_gather = 0` if the host cannot provide it.

The service is deployed by ff-deploy (push the config and image bump to `main` in one commit):

- **Image** `registry.digitalocean.com/feral-file/apps:ff-eth-logs-<tag>` (built from `tools/docker/Dockerfile` by the `Build Image` workflow); the container starts the binary with `-config /app/config.yaml`. The image also carries `db/` at `/app/db`.
- **Config** rendered to the host and mounted at `/app/config.yaml`; every key of `config.yaml.sample` is rendered, secrets included (the file is `0400`, owned by the container user).
- **Secrets** (ff-deploy vault, `make vault-edit APP=eth_logs`): the Chainstack WebSocket URL with its key — the same endpoint the indexer uses — and the database password.
- **Schema**: on every deploy the role extracts `/app/db/init_pg_db.sql` from the pinned image and applies it with `psql -v ON_ERROR_STOP=1` (idempotent) after PostgreSQL is ready and before the service container is recreated. A schema change therefore ships in the same image as the code that needs it; if it touches `eth_logs` indexes, stop ingestion for the deploy.
- **PostgreSQL**: its own container and its own volume (≈ 250 GB); excluded from backups — the GCS export is the DR source.
- **Network**: both containers on the backend network only; port 8545 is not published outside it (no auth on the endpoint).
- **Restart policy**: the container must restart on exit — ingestion errors are fatal by design and recovery is "restart from the cursor".
- **Healthcheck**: `GET /health` returns 200 whenever the database answers, including during catch-up.

Deployment order for a schema change is enforced by the role: schema first (from the new image), then the container running that image. Migrations that are not `IF NOT EXISTS`-safe do not exist yet; when one is needed, add it under `db/migrations/` and have the role apply it in order before `init_pg_db.sql` — do not rely on the service to migrate at start-up.

## 8. Backfill on the deployed host

Run the backfill as a **one-off container with the service stopped**, never `docker compose exec` into the live container:

```bash
docker compose stop ff-eth-logs                       # the writer lock refuses a backfill against a live ingestion anyway
docker compose run --rm ff-eth-logs backfill -config config.yaml -dir /data/v1
docker compose start ff-eth-logs                      # resumes at the published head + 1
```

`make backfill DIR=/data/v1` does the same against the dev compose stack. While the backfill runs, API reads — including on an API-only replica (`ethereum.ingestion_enabled=false`) sharing the database — are refused with `out of warehouse scope: warehouse is under maintenance (a backfill is reloading it); ask a node` and `/health` returns `503 {"status":"maintenance"}`: the backfill holds the maintenance lock exclusively and every read snapshot takes it shared. The stages take the warehouse writer lock (`ingest`/`backfill` are mutually exclusive: `another writer holds the warehouse (serve ingestion or a backfill stage is running)`), operate only inside the manifest's block interval — rows the tail wrote above the export end are neither counted, deleted nor verified — and `finish` merges its verified interval into the existing coverage rather than lowering a head the tail has moved past.

## 9. Re-exporting from BigQuery

When history must be regenerated (a new signature or shape rule, a corrupt export, or simply a fresher starting point), re-run `warehouse_extract.sql` — the full script and its rationale are in [probe_2026-08.md](probe_2026-08.md):

- One BigQuery script, one pass over `bigquery-public-data.crypto_ethereum.logs` (7.4 B rows): materialise the shaped set into `indexer-eth-logs.eth_warehouse.logs`, export `logs/part=NNN/` and `blocks/` to a new prefix under `gs://eth-logs/eth-log-warehouse/`, compute the sizing stats.
- Cost and time of the 2026-08-28 run: **134 s**, 3,622 GB billed ≈ **$20.6 list, ≈ $14 after the free TiB**; set "Maximum bytes billed" to 4 TB as a hard stop before running.
- Change the `bucket` DECLARE to a new version prefix (`v2`, …) rather than overwriting `v1`; keep the shape rules in the `WHERE` clause identical to `eventset.Keep`, or change both in the same PR.
- Verify with the external-table round-trip (row count and distinct `(block_number, log_index)` equal) before loading.
