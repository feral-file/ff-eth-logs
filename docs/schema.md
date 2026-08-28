# Database Schema

This document describes the FF Eth Logs warehouse schema: tables, partitions, indexes, the size model, and migration notes. The source of truth for a fresh database is `db/init_pg_db.sql`; the DDL the code executes at runtime lives in `internal/logstore/schema.go`.

## 1. Overview

PostgreSQL 18, three tables, no foreign keys, no JSON:

- `eth_blocks` — one row per confirmed block: hash and timestamp, stored once and joined on read
- `eth_logs` — one row per stored log, range-partitioned per 1,000,000 blocks
- `ingest_cursor` — the single-row covered interval `[coverage_start, block_number]`

Design rules: `bytea` everywhere (hex text would double every column); block hash and timestamp are not on the log row; every stored log has the shape `internal/eventset` accepts, so the table is narrower than the chain's `Transfer` signature set.

## 2. Tables

### eth_blocks

| Column | Type | Description |
| --- | --- | --- |
| number | bigint | Primary key |
| hash | bytea | 32-byte block hash as reported by the node (wire hash) |
| ts | bigint | Unix seconds |

Only confirmed blocks (at least `ethereum.confirmation_blocks` behind the tip) are present, and every height from the first loaded block to the cursor has a row: the reader joins it for `blockHash` / `blockTimestamp`, and `WriteRange` refuses a batch whose block rows do not cover its range.

**Indexes**: `eth_blocks_hash` on `(hash)` — the `eth_getLogs {blockHash}` lookup.

### eth_logs

| Column | Type | Description |
| --- | --- | --- |
| block_number | bigint | Partition key; part of the primary key |
| log_index | integer | Log position in the block; part of the primary key |
| tx_index | integer | Transaction position in the block |
| tx_hash | bytea | 32 bytes |
| address | bytea | 20 bytes, emitting contract |
| topic0 | bytea | 32 bytes, event signature (NOT NULL) |
| topic1 | bytea | 32 bytes or NULL when the log has fewer topics |
| topic2 | bytea | same |
| topic3 | bytea | same |
| data | bytea | Raw data; empty (not NULL) for ERC-721 `Transfer` |

`PRIMARY KEY (block_number, log_index)`, `PARTITION BY RANGE (block_number)`.

**Why bytea** — every hash, address and topic is fixed-width binary; storing hex text would double the heap and every index key, and the Parquet export already carries `BYTES`, so the backfill is a straight COPY.

**Why no block_hash or timestamp per row** — both are per-block facts joined from `eth_blocks` at read time: 33 + 8 bytes saved per row across 400 M rows, and a rewound block rewrites one `eth_blocks` row instead of thousands of log rows.

**NULL topics** — a wildcard filter position is `topicN IS NOT NULL`, which reproduces go-ethereum's rule that an N-position filter matches only logs with at least N topics.

### ingest_cursor

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `smallint` PK, `CHECK (id = 1)` | exactly one row |
| `coverage_start` | `bigint NOT NULL` | oldest block whose blocks row and logs are all stored |
| `block_number` | `bigint NOT NULL` | the head: newest such block; `CHECK (coverage_start <= block_number)` |
| `updated_at` | `timestamptz NOT NULL DEFAULT now()` | when the interval last moved |

The row is the covered interval, and the API answers only inside it. Written in the same transaction as the blocks and logs it accounts for (`logstore.WriteRange`), so it is never ahead of the data; a write must be contiguous with the interval (`from ≤ head+1` and `to+1 ≥ coverage_start`, else `ErrCoverageGap`) and extends it. Absent before the first write or backfill `finish`; `eth_blockNumber` and `eth_getLogs` answer `out of warehouse scope: warehouse is empty` until then. `finish` publishes `[MIN(eth_blocks.number), MAX(eth_blocks.number)]` only after verifying the load. `rewind` lowers the head and refuses to move it forward or below `coverage_start`.

## 3. Partitions

`eth_logs` is partitioned per **1,000,000 blocks** (`logstore.PartitionBlocks`), named `eth_logs_p%03d` (`eth_logs_p000` covers blocks 0–999,999). `db/init_pg_db.sql` creates 40 up front — to block 40,000,000, roughly 2031 at 12-second blocks. `logstore.WriteRange` probes `to_regclass` for every partition a batch touches and creates a missing one inside the write transaction with `logstore.PartitionDDL` (`CREATE TABLE IF NOT EXISTS … PARTITION OF eth_logs FOR VALUES FROM (lo) TO (lo + 1000000)`), so running out of pre-created partitions is not a failure mode.

The boundaries match the BigQuery export directories (`logs/part=NNN/` = blocks NNN×1,000,000 … +999,999), so the backfill loads one directory into one partition and can skip a directory whose range already has rows.

## 4. Indexes

`logstore.SecondaryIndexes`, one per query shape and nothing speculative:

```sql
CREATE INDEX eth_logs_t1   ON eth_logs (topic1, block_number) WHERE topic1 IS NOT NULL;
CREATE INDEX eth_logs_t2   ON eth_logs (topic2, block_number) WHERE topic2 IS NOT NULL;
CREATE INDEX eth_logs_t3   ON eth_logs (topic3, block_number) WHERE topic3 IS NOT NULL;
CREATE INDEX eth_logs_addr ON eth_logs (address, block_number);
```

plus the primary key and `eth_blocks_hash`. The block range sits in every key so a bounded scan never touches the heap for out-of-range rows. Not indexed, on purpose: `tx_hash` (the only tx lookup in the indexer is bounded by address + block) and `topic0` alone (every served query carries a block range or a more selective column).

**Rule**: the four statements in `db/init_pg_db.sql` must be byte-identical to `logstore.SecondaryIndexes`. The backfill drops them (`prepare`) and recreates them (`finish`) from the Go list, and `TestSchemaMatchesInit` compares the two; changing one without the other fails the test.

## 5. Size model

Per row: ERC-721 `Transfer` columns 8 + 4 + 4 + 33 + 21 + 33 + 33 + 33 + 33 + 1 ≈ 200 B plus a 24 B tuple header ≈ **230 B heap**; ERC-1155 rows add 64–1,152 B of `data` (measured: `TransferSingle` 64 B, `TransferBatch` avg 257 B, `URI` avg 135 B; 7.52 GB of `data` in total, 6.7 GB of it `TransferSingle`). Indexes ≈ **260 B/row**: PK ≈ 30 B, each topic index ≈ 60 B, address index ≈ 55 B. All-in **≈ 510 B/log**.

Measured on 2026-08-28 ([probe](probe_2026-08.md)): **402,266,375** logs (genesis → block 25,842,829) → heap ≈ 99 GB + indexes ≈ 106 GB ≈ **205 GB**. Growth at the 12-month average of 2.9 M logs/month ≈ **1.5 GB/month**. BigQuery's logical size for the same rows is 98 GB; the 2× is index overhead, as expected.

Levers if a future measurement comes in high, cheapest first: `topic0` as a `smallint` signature id; drop `tx_index`; skip the `topic2` / `topic3` indexes for `MetadataUpdate` / `URI` rows; `fillfactor=100` on closed partitions. None is needed at 205 GB.

## 6. Migrations

Migrations live in `db/migrations/` as sequentially numbered `NNN.sql` and are mirrored into `db/init_pg_db.sql`, which stays the complete schema for a fresh database and for the integration tests.

- `001.sql` — initial schema; identical to `db/init_pg_db.sql` at this version (`\ir ../init_pg_db.sql`, resolved relative to the migration file, so `psql -f db/migrations/001.sql` works from any directory). Apply one or the other on a fresh database.

**Deployment ordering rule**: run a migration before deploying the binary that depends on it; the code has no schema bootstrap of its own beyond on-demand partitions. A migration that rebuilds an `eth_logs` index on the full table is a long, write-blocking operation — schedule it with ingestion stopped and see the `maintenance_work_mem` note in [operations](operations.md).

**Guidelines**: keep migrations transactional where PostgreSQL allows it; update `docs/schema.md` and any integration-test fixture in the same change; keep the secondary-index statements identical in `schema.go` and the init file.

## 7. Indexing Strategy

| Query shape (indexer call site) | Filter | Index |
| --- | --- | --- |
| Owner scan, owner as `from` | `topics[1] = owner`, block range | `eth_logs_t1` |
| Owner scan, owner as `to` / ERC-1155 `from` | `topics[2] = owner` | `eth_logs_t2` |
| Owner scan, ERC-1155 `to` | `topics[3] = owner` | `eth_logs_t3` |
| Contract provenance (ERC-721 / ERC-1155 / CryptoPunks) | `address = contract`, block range | `eth_logs_addr` |
| ERC-721 token provenance | `address = contract AND topics[3] = tokenId` | `eth_logs_addr` or `eth_logs_t3`, planner's choice |
| Per-block reads (tail ingestion replay, deletes) | `block_number BETWEEN a AND b` | primary key on the pruned partition |
| `eth_getLogs {blockHash}` | `eth_blocks.hash = h` → one block | `eth_blocks_hash`, then the primary key |
| Reorg rewind | `block_number > n` | primary key |

Every read goes through one statement (`logstore.selectLogs`) that joins `eth_blocks` on `number` and orders by `(block_number, log_index)`; the PK delivers that order on a single partition, and cross-partition results are merged by the planner.

## 8. Data Retention

None. Nothing is expired or compacted: the warehouse is the full stored history from block 937,821 (the first log in the set) to the head. It is also not a system of record — the GCS Parquet export plus tail re-ingestion rebuilds it entirely, so the database is excluded from backups and can be dropped and reloaded ([operations](operations.md)).
