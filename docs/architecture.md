# Architecture

This document describes the system architecture, components, and data flow of FF Eth Logs.

## 1. Overview

FF Eth Logs is one binary (`cmd/ff-eth-logs`) over one PostgreSQL database:

1. **Backfill** loads the one-off BigQuery Parquet export (genesis → the export head) into partitioned tables and sets the cursor.
2. **Tail ingestion** follows the chain head over a `newHeads` subscription and, for every block that has reached the confirmation depth, fetches the warehouse's event set and writes it in one transaction with the block's metadata and the cursor.
3. **The JSON-RPC server** answers `eth_getLogs` from the tables with go-ethereum's filter semantics, bounded by the cursor.

**Deployment model**: `serve` runs the HTTP server and tail ingestion as goroutines in one process (`errgroup`); either failing stops the other, and the supervisor restarts the process from the durable cursor. `backfill` and `rewind` are one-off commands against the same database.

The ingestion code — head tracking, confirmation lag, reorg accounting, catch-up batching, dense-block fallback, error classification and retry policy — is ported from ff-indexer-v2 (`internal/providers/ethereum` at 58d4b04) so both services read the chain identically. Only the sink differs: the indexer enqueues parsed events, the warehouse stores raw logs.

## 2. System Components

### Infrastructure Services

1. **PostgreSQL 18** — `eth_blocks`, `eth_logs` (range-partitioned per 1,000,000 blocks), `ingest_cursor`. See [schema](schema.md).
2. **An Ethereum WebSocket endpoint** — `newHeads`, `eth_getLogs`, `eth_getBlockByNumber`, `eth_getBlockReceipts`, `eth_blockNumber` on one connection (Chainstack, shared with the indexer).

### Application packages

| Package | Responsibility |
| --- | --- |
| `cmd/ff-eth-logs` | Subcommand dispatch (`serve` default, `backfill`, `rewind`), flags `-config` / `-env` / `-dir` / `-stage` / `-to`, signal handling, wiring |
| `internal/config` | YAML + `FF_ETH_LOGS_*` loading, defaults, validation |
| `internal/logger` | zap with a `component` context field (`http-server`, `ingestion`, `backfill`); Sentry at error level |
| `internal/eventset` | The nine signatures (computed with `crypto.Keccak256Hash` at init), `Topics()` for the ingestion filter, `Keep` (store this log?) and `IsWarehouseSignature` (can we serve this filter?) |
| `internal/chain` | `EthClient` interface and `RealEthClient` (ethclient with retries), `BlockHead`, error classifiers, `Paginator` |
| `internal/ingestion` | `Run` (start-block resolution), `Subscriber` (head loop, reorg accounting, batching), `Fetcher` (topic-set fetch with receipts fallback) |
| `internal/logstore` | `Store` (pool, cursor, `WriteRange`, `Rewind`, `SetCursor`), `FilterLogs` reader, `BlockByHash`, partition and index DDL |
| `internal/rpcapi` | `API` (`eth` namespace), `FilterCriteria` decoder, `ScopeError`, `Server` (`rpc.Server` at `/`, `/health`) |
| `internal/backfill` | `Loader` stages `Prepare` / `Logs` / `Blocks` / `Finish` over a Parquet export |
| `internal/mocks` | Generated `MockEthClient`, `MockClock` |

## 3. Tail Ingestion

### 3.1 Start block

`ingestion.Run` resolves where to start: the cursor + 1 when a cursor exists (the cursor is the last written block) — a non-zero `ethereum.start_block` is then a startup error (`ethereum.start_block=… is set but the warehouse already has a cursor at …; unset it … or … run \`ff-eth-logs rewind -to …\``), because a persistent setting would bypass the durable cursor on every restart; else `start_block` when non-zero (a first run without a backfill); else the current chain head, with the warning `No cursor and no start_block: starting at the chain head; history below it needs a backfill`. Then `Starting ethereum ingestion` (fields `fromBlock`, `confirmationBlocks`, `maxCatchupBlocks`).

### 3.2 Head subscription and the `Header.Hash()` trap

Heads arrive over `eth_subscribe("newHeads")` through the **raw** RPC client into `chain.BlockHead{Number, Hash, ParentHash, Timestamp}`, not through `ethclient.SubscribeNewHead`. Reason: `ethclient` decodes notifications into `types.Header` and drops the node-reported `hash`; recomputing it with `Header.Hash()` does not reproduce it on current mainnet (the struct lacks fields newer than this go-ethereum version's RLP encoding — verified live in the indexer: every local hash differed while each head's `parentHash` matched the node hash of its predecessor). Parent-hash continuity must be checked against wire hashes, so every hash the subscriber compares comes off the wire (`newHeads` or `eth_getBlockByNumber`). The timestamp rides along so steady-state blocks need no `eth_getBlockByNumber`.

The head channel holds 64 notifications; each loop iteration takes one head and drains whatever is queued behind it, then plans **one** range (`planRange`). go-ethereum queues up to 20k more behind the buffer and then fails the subscription, which ends the process.

### 3.3 Confirmation lag

`Config.ConfirmationBlocks` (default 2): a block is written only once `tip - ConfirmationBlocks` reaches it, so blocks land about two blocks (~24 s) plus one fetch round-trip after the tip. That lag **is** the reorg strategy for the common case: post-merge mainnet reorgs are almost always one block deep and never reach a written block. A reorg that does reach one is recovered by rewinding to the verified common ancestor (3.4).

### 3.4 `streamState` and reorg accounting

```go
type streamState struct {
    next       uint64                      // first unwritten block
    lowerBound uint64                      // the caller's fromBlock: nothing below is ever written
    tip        uint64                      // highest head seen
    heads      map[uint64]*chain.BlockHead // heads at heights not yet written (+ the last written one)
}
```

Rules, by height relative to `next`:

- **`record`** — below `lowerBound`: ignored (a future `start_block` is a hard boundary). At or above `next`: reconciled, then retained; a replacement (different hash at the same height) overwrites and truncates everything above it — `Ethereum shallow reorg absorbed within confirmation lag` at info. Below `next`: a head for an already-written height. If its hash is the persisted one it is a late re-delivery (ignored); otherwise the canonical header decides — if the node still holds what we wrote the head is a stale branch (ignored), else the written block was replaced and `recoverDeepReorg` runs: walk down from it comparing canonical headers with the persisted `eth_blocks` hashes until they agree, `Rewind` the store to that verified ancestor, restart the window there (`next = ancestor+1`), re-record the head and re-fetch. Logged as `ethereum reorg deeper than confirmation lag: rewinding to the verified common ancestor` at **error** level (fields `replacedHeight`, `lastWritten`, `ancestor`, `blocksDropped`). A fork deeper than 1,024 blocks or below the covered interval is fatal.
- **`reconcile`** — when a head's parent disagrees with the retained head at the previous height, walk canonical heads by number (`eth_getBlockByNumber`, wire hashes) down from the parent until the retained chain matches again, replacing stale retained heads and bridging heights no head was received for (only once a written boundary is retained to walk down to). A head more than `maxBridgeWalk` (64) blocks above everything retained — the first head after a restart behind a gap — is not walked: it is verified by number with one call (`Reconciling headers down to the retained chain` logs the walk when it does run), and the gap is verified during the catch-up instead, see 3.7. Reaching a written height with a different hash is a deep reorg: `recoverDeepReorg` walks down comparing canonical headers with the hashes persisted in `eth_blocks` until they agree, rewinds the store to that verified common ancestor, restarts the window there and re-records the head, so the canonical blocks are re-fetched (a fork deeper than 1,024 blocks or below coverage is fatal). Returns `stale` (the incoming head's ancestry is not what the node holds canonical: the head is dropped) and `replaced` (a retained head was swapped: `truncateAbove(n-1)`). Fetches happen only on a known mismatch and are bounded by the retained window.
- **`truncateAbove(h)`** — forget every retained head above `h` and make `h` the tip: the confirmation depth restarts from the replacement branch.
- **`planRange`** — first a preflight of the catch-up bound on the highest received head (`checkCatchupBound`): if `MaxCatchupBlocks > 0` and `highest - next + 1 > MaxCatchupBlocks`, return `ErrCatchupTooLarge` (fatal; the bound covers the whole gap to the tip, pending window included) *before* any head is reconciled — with the resume bridge retained, reconcile would otherwise walk the whole gap in `eth_getBlockByNumber` calls first. The same check runs again on the tip after recording. Else `to = tip - ConfirmationBlocks`; nothing to do if `to < next`. The boundary head `to` must be retained with a canonical hash for later reconciliation — fetched with `HeadByNumber` when the subscription did not deliver it (first head after a resubscribe).
- **`advance(to)`** — `next = to + 1`; forget heads below the last written height.

A deep reorg that spans a process restart is caught at start: `verifyResumePoint` compares the persisted hash of the block the stream resumes after (`cursor`) with `eth_getBlockByNumber` before the first head is processed — a mismatch runs the same verified-ancestor rewind, a match retains the canonical head so reconciliation has its bridge from the first tip on (`Written head is no longer canonical at start; a reorg happened while the process was down` at warn, then the error line above).

### 3.5 Catch-up and batching

`ingestRange(from, to)` logs `Ethereum ingestion catching up to head` when the span is more than one block, then walks it in **10-block batches** (`catchupBatchBlocks`), committing each batch — blocks, logs and cursor — with one atomic `WriteRange` before fetching the next, so a later failure never re-scans it or trips the catch-up bound on it. `Ethereum ingestion catch-up progress` every 50 batches. Ten blocks is the size because the ingestion filter is topic0-only and returns ~470 raw matches per mainnet block on average (ERC-20 `Transfer` shares the ERC-721 signature and is only discarded by `eventset.Keep`); a 20-block batch tripped Infura's 10k-result cap live.

Per batch (`ingestBatch`): `blockMetadata` first (retained heads, else `eth_getBlockByNumber`) → `Fetcher.FetchIngestionLogs(from, to)` → consistency check → `eventset.Keep` → `WriteRange`. The metadata is the chain version the batch is committed to. The check has two halves, because logs and headers are separate reads and a reorg between them would otherwise pair old-branch logs with new-branch hashes — or store a new-branch hash for a block whose only warehouse event exists on that branch and was never fetched: every *raw* log's `blockHash` (before the shape filter; on mainnet almost every block has an ERC-20 Transfer in the raw filter) must equal the metadata hash for its height, and every block that returned no raw log is re-read from the node after the log fetch and must still carry the metadata hash. Disagreeing heights have their retained heads dropped (refetched canonical) and the whole batch is fetched again (`Block hashes moved between the log fetch and the header fetch; refetching the batch` at warn), up to three attempts, then a fatal error. `logstore.WriteRange` re-checks the log/block pairing as a last line of defense. Steady state is the same path with a 1-block range.

### 3.6 Fetching and the dense-block fallback

`Fetcher` issues `eth_getLogs{fromBlock, toBlock, topics: [[all nine signatures]]}` — no addresses, exactly the indexer's ingestion filter — through `chain.Paginator`: start at `min(1,000,000, span_cap + 1)` blocks, halve on a too-many-results or range-cap rejection (sleep 1 s between attempts), ramp back ×2 after successes, and hoist a discovered span cap to the rest of the walk. When the walk halves down to a single block and the provider still refuses it (`SingleBlockOverflowError`), the block is read from `eth_getBlockReceipts` filtered by the same topic0 set — `Dense block served from receipts (eth_getLogs result cap)` (fields `block`, `receiptLogs`, `matched`) — and the blocks on either side go through the normal path recursively. The densest NFT-only block in the backfill had 20,803 logs, above the 10k result cap, so this path is load-bearing. Logs are stable-sorted by `(BlockNumber, Index)` before return.

### 3.7 Block metadata and chain continuity

`blockMetadata` builds the `eth_blocks` rows for the batch from the retained heads when the subscription delivered them (steady state) and from `eth_getBlockByNumber` otherwise (catch-up), retaining the fetched head.

Every batch is also checked for **parent continuity**: each block's `parentHash` must equal the hash held for the height below, starting from `from-1` (the last written block, or the start anchor). This is what makes a catch-up over a long gap trustworthy without walking its ancestry up front — every batch links to the previous one and the link is committed with the batch. A mismatch inside the batch means the chain moved between two fetches: the headers from that height are dropped and the batch retried (`Chain moved under the batch (parent mismatch); refetching its headers`, three attempts). A mismatch at the boundary means the block below was replaced: if it is stored, the deep-reorg recovery runs (3.4) and the range is replanned from the next head; if it is only the start anchor of an empty warehouse, the anchor is re-fetched (`Start anchor was replaced by a reorg; re-anchoring`) and the range replanned.


### 3.8 Error classification and retries

`chain.RealEthClient` wraps every call in the indexer's policy verbatim: exponential backoff 5 s initial, 30 s max, 5 min total, ×2, 50 % jitter. Retryable: network timeouts, `ECONNREFUSED` / `ECONNRESET` / `ENETUNREACH` / `EHOSTUNREACH`, temporary DNS failures, and lowercase substrings (`connection refused`, `connection reset`, `i/o timeout`, `broken pipe`, `rate limit`, `too many requests`, `service unavailable`, `bad gateway`, `gateway timeout`, `temporarily unavailable`, `eof`, `internal error`, `502` / `503` / `504`, …). Context errors and everything unknown are permanent. `retryable ethereum error encountered` is logged at warn with `operation` and the URL reduced to scheme and host.

Range-cap and too-many-results rejections (`IsBlockRangeCapError`, `IsTooManyResultsError`) are matched case-insensitively against the observed provider phrasings (Infura `range N exceeds limit of 10000`, Chainstack `Block range limit exceeded` with `-32602`, drpc `query returns too many logs`, …) and drive pagination rather than failure.

### 3.9 Fatal on error

Any subscription error (`new heads subscription error: …`), fetch failure after the retry budget, sink failure or too-large catch-up returns from `Subscriber.Run`, which ends the process (`ff-eth-logs stopped with error`). There is no in-process reconnect: the supervisor restarts the process and it resumes from the cursor. The API goroutine stops with it so a stalled head is never served silently.

## 4. Serving path

```text
POST /  →  rpc.Server (geth)  →  API.GetLogs(FilterCriteria)
              │ decode: FilterCriteria.UnmarshalJSON (geth copy)      -32602 on failure
              ├ len(topics) > 4                                        "exceed max topics"
              ├ checkTopicScope: topics[0] ⊆ eventset                  ScopeError
              ├ store.Head()                                           ScopeError "warehouse is empty"
              ├ resolveRange: blockHash | tags | bounds ≤ head         geth errors / ScopeError / []
              └ store.FilterLogs(q, max_results) under query_timeout   "query returned more than N results"
                    SELECT … FROM eth_logs l JOIN eth_blocks b ON b.number = l.block_number
                    WHERE l.block_number BETWEEN $1 AND $2
                      [AND l.address = ANY($3::bytea[])]
                      [AND l.topicN = ANY($k::bytea[])]                          -- per valued position; a wildcard adds nothing
                    ORDER BY l.block_number, l.log_index LIMIT max_results + 1
```

A wildcard position adds no predicate: the vendor imposes no existence constraint on wildcard positions (measured, see [api design](api_design.md)), and a valued position on a NULL column never matches, so a log without that topic is excluded by the value test alone. `LIMIT max_results + 1` lets the reader tell "exactly the limit" from "more" and return an error rather than a truncated slice. `blockHash` filters resolve through `eth_blocks_hash` to a single-block range.

## 5. Data Flow

```text
        Ethereum mainnet (Chainstack WebSocket)              BigQuery public dataset
        newHeads · eth_getLogs · eth_getBlockByNumber          crypto_ethereum.logs/blocks
        eth_getBlockReceipts                                             │
                    │                                                    │ warehouse_extract.sql (one pass)
                    ▼                                                    ▼
          internal/ingestion                                  gs://eth-logs/eth-log-warehouse/v1
          Subscriber ─ planRange ─ Fetcher ─ eventset.Keep     logs/part=NNN/*.parquet · blocks/*.parquet
                    │                                                    │ gcloud storage cp -r
                    │ WriteRange (blocks + logs + cursor, one tx)        ▼
                    ▼                                          internal/backfill (prepare → logs → blocks → finish)
             ┌──────────────────────────────────────────┐               │ COPY via staging, sorted per partition
             │ PostgreSQL: eth_blocks · eth_logs_pNNN   │ ◄─────────────┘
             │   ingest_cursor [coverage_start, head]   │
             └──────────────────────────────────────────┘
                    │ Head · FilterLogs · BlockByHash
                    ▼
          internal/rpcapi  (rpc.Server: eth_getLogs · eth_blockNumber · eth_chainId · GET /health)
                    │
                    ▼
          ff-indexer-v2 routing EthClient  (history ≤ head here; tip range, eth_call, receipts → vendor)
```

## 6. Backfill

`ff-eth-logs backfill -dir <export>` runs four idempotent, resumable stages (`-stage all|prepare|logs|blocks|finish`):

| Stage | Work | Idempotency |
| --- | --- | --- |
| `prepare` | `DROP INDEX IF EXISTS` each of `logstore.SecondaryIndexes` (`Dropped index for bulk load`) | already dropped is fine |
| `logs` | For each `logs/part=NNN/` in order: ensure the partition (`PartitionDDL`), then in one transaction `CREATE TEMP TABLE staging_logs (LIKE eth_logs) ON COMMIT DROP`, COPY every `*.parquet` file into it, `INSERT INTO eth_logs SELECT * FROM staging_logs ORDER BY block_number, log_index` so the heap is chain-ordered (`Partition loaded` with `files`, `rows`, `took`) | a partition is skipped only when the database already holds the manifest's row count **and** `backfill_units` records the same manifest fingerprint for it (`Partition already loaded from this export, skipping`); any other non-empty state is cleared and reloaded (`… reloading it`); files are verified against the manifest before a row is read; a failed directory rolls back to empty |
| `blocks` | Read `manifest.json`, then COPY every `blocks/*.parquet` into `eth_blocks` in one transaction, dropping rows outside the manifest interval (the live blocks table can run ahead of the logs extract; the manifest, not the blocks export, bounds coverage) (`Blocks load progress` every 500 files, `Blocks loaded`) | skipped only when the interval's row count and the recorded manifest fingerprint both match; otherwise cleared and reloaded |
| `finish` | Verify the load against `manifest.json` (written from the export's source — BigQuery counts, GCS checksums): `eth_blocks` equals the manifest interval and row count; every partition in the interval exists and holds the manifest's rows in its Parquet footers and in the database; every listed file matches size and MD5 and no unlisted Parquet file exists — then recreate the indexes (`Index ready` with `took`), `ANALYZE eth_logs, eth_blocks`, publish coverage `[manifest first, manifest last]` (`Backfill finished; coverage published`) | `already exists` on an index is tolerated; refuses with `backfill is not complete, cursor not set: …` (`manifest.json is required`, `no blocks loaded`, `eth_blocks holds … manifest.json says …`, `part NNN files hold X rows, manifest.json says Y`, `part NNN has X rows in the database, manifest.json says Y`, `… differs from manifest.json`) and leaves the cursor unset |

Sorting happens in PostgreSQL (an external sort bounded by `work_mem`) because the busiest 1 M-block partition holds ~60 M rows. Parquet rows map straight onto the COPY columns: hashes and topics are already `BYTES`, absent topics are NULL, `data` NULL becomes empty, the export's `block_timestamp` is ignored (`eth_blocks.ts` carries it).

## 7. Scaling Notes

- **One writer** — tail ingestion assumes it is the only process writing the cursor; run one `serve` with ingestion enabled per database. API-only replicas (`ethereum.ingestion_enabled=false`) can share the database.
- **Partitioning** — `eth_logs` is range-partitioned per 1,000,000 blocks (`eth_logs_p000` … `p039` created by the init script; `WriteRange` creates a missing one inside the write transaction with `PartitionDDL`). Every query carries a block range, so the planner prunes to the touched partitions; the backfill loads one export directory per partition.
- **Size model** (measured 2026-08-28, [probe](probe_2026-08.md)) — ≈ 230 B heap per row (columns + tuple header) and ≈ 260 B of index entries per row (PK ≈ 30 B, each topic index ≈ 60 B, address index ≈ 55 B): **≈ 510 B/log all-in**. 402,266,375 logs → heap 99 GB + indexes 106 GB ≈ **205 GB**. Growth ≈ 2.9 M logs/month ≈ **1.5 GB/month**; a new partition every ~4.5 months.
- **Index cardinality** — 7.1 M distinct `topic1`, 12.5 M `topic2`, 57.3 M `topic3`, 420 k contracts: owner and contract lookups are selective; the block range in every index key keeps a bounded scan off the heap for out-of-range rows.
- **Cost of a wide query** — a filter that pins only `topic0` over a wide range is a partition scan; `rpc.max_results` (100k) and `rpc.query_timeout` (60 s) bound it. A genesis-wide `Transfer` query is 293 M rows and is refused by the cap.

## 8. Relationship to the indexer

What stays on the vendor RPC in ff-indexer-v2:

- **Tip ingestion** — its own `newHeads` subscription and per-block `eth_getLogs` (the same code this service ports); the indexer's cursor and the warehouse head are independent.
- **State calls** — `eth_call` (`ownerOf`, `tokenURI`, `balanceOf`, `punkIndexToAddress`), `GetContractDeployer` (`eth_getCode` bisection and block transactions).
- **Receipts** — `eth_getTransactionReceipt` for the CryptoPunks repair path.
- **The residual range** above the warehouse head for any historical walk.

What moves here, through the indexer's routing `EthClient`: every historical `eth_getLogs` — owner scans (owner in topic 1 / 2 / 3, three queries per address), ERC-721 provenance (`address` + `Transfer` / `MetadataUpdate`, token id in topic 3), ERC-1155 provenance, `TokenExists` / `ReplayBalances` / `BalanceAndEventsForOwner`, CryptoPunks provenance — each collapsing from ~2,600 windows to one query, plus block timestamps (`blockTimestamp` on every log) that today cost an `eth_getBlockByNumber` per distinct block. Once a call site is warehouse-served in production, the machinery that existed only to ration vendor calls (credit guard, address throttle, scan-session windows) becomes removable on the indexer side.
