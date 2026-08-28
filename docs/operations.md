# Operations

Runbook for **FF Eth Logs**: first deployment, the backfill, reorg and catch-up procedures, monitoring, and deployment through ff-deploy. Commands assume the binary on the host or `docker compose exec`; every subcommand takes `-config <file>` and `-env <dir>`.

## 1. First deployment

### 1.1 Create the database

Apply the schema once, on the empty database the service will use:

```bash
psql -h <db-host> -U postgres -d ff_eth_logs -f db/init_pg_db.sql
```

(With ff-deploy the PostgreSQL container mounts the file as its init script.) The script is idempotent. Provision **≈ 250 GB** of disk: the load ends at ≈ 205 GB and grows ≈ 1.5 GB/month.

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

### 1.3 Run the backfill

With the service **not running** (or `ethereum.ingestion_enabled=false`) on the empty warehouse:

```bash
ff-eth-logs backfill -config /app/config.yaml -dir ./data/v1
```

Stages run in order and are each idempotent (`-stage prepare|logs|blocks|finish` runs one):

1. `prepare` — drops the four secondary indexes (`Dropped index for bulk load`).
2. `logs` — one transaction per `part=NNN` directory: COPY into a temp staging table, `INSERT … ORDER BY (block_number, log_index)` into the partition (`Partition loaded` with `rows` and `took`; `Partition already loaded, skipping` on re-run). The busiest partition holds ~60 M rows and is sorted by PostgreSQL.
3. `blocks` — all 4,048 files into `eth_blocks` in one transaction (`Blocks load progress` every 500 files).
4. `finish` — recreates the indexes (`Index ready` per index with `took`), `ANALYZE`, sets the cursor to `MAX(eth_blocks.number)` (`Backfill finished; cursor set`).

**Durations are not yet measured** — record the `took` fields of the first production run here. Session-level PostgreSQL settings that matter: `maintenance_work_mem` for the four index builds over 400 M rows (e.g. `maintenance_work_mem = '2GB'`), `work_mem` for the per-partition sort (anything below the partition size spills to an external sort, which is correct but slower), and enough WAL headroom (`max_wal_size`) for a multi-GB COPY per transaction. Set them in `postgresql.conf` for the load or `ALTER SYSTEM`, and reset afterwards.

A failed stage rolls its transaction back; re-run the same command and it resumes at the first unloaded partition.

### 1.4 Start serving and let the tail catch up

Start `serve`. Ingestion resumes at cursor + 1 = **25,842,830** and logs `Ethereum ingestion catching up to head` with `blocks` = the gap. The gap is bounded by `ethereum.max_catchup_blocks` (default 50,000 ≈ 7 days at ~7,200 blocks/day): **if the export is older than that at first start, raise `ethereum.max_catchup_blocks` above the gap for the first run** (the process otherwise exits with `ingestion catch-up exceeds max_catchup_blocks`), then set it back. `ethereum.confirmation_blocks` must stay below it. The catch-up walks 10-block batches, committing each; the plan estimated ~80 min for a 14k-block gap — measure and record it.

### 1.5 Verify

```bash
curl -s http://<host>:8545/health
# {"status":"ok","head":<block>,"empty":false,"chain_id":1}
```

`head` must be within a few blocks of the chain tip once the catch-up finishes. Then send the same `eth_getLogs` body to the warehouse and to the vendor for an in-scope filter at or below the head — for example a CryptoPunks `PunkTransfer` query on `0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb` over a closed range — and diff the arrays; they must be identical.

## 2. Deep reorg

**Signal** — an error-level line (reaches Sentry):

```text
ethereum reorg deeper than confirmation lag: a written block was replaced
  height=<h> lastWritten=<n> newHash=0x…
  hint="logs for the affected heights are stale; run `ff-eth-logs rewind -to <height-1>` and restart"
```

The block at `height` was written and the chain has since replaced it; its logs (and everything above it) are stale until rewound. The service keeps running and keeps writing new blocks — nothing is repaired automatically.

**Procedure**

1. Stop the service.
2. Rewind to the block **below** the lowest reported height. `-to` is the last block to keep; everything above it is deleted and re-ingested:
   ```bash
   ff-eth-logs rewind -config /app/config.yaml -to <height-1>
   ```
   Output `Rewound warehouse to=<height-1>`. `rewind to N: cursor is not above it (nothing to rewind)` means the cursor is already at or below `N`.
3. Start the service. It resumes at `height`, re-fetches through the current confirmed head, and the replaced range is overwritten.

Post-merge mainnet reorgs are almost always one block deep, so with `confirmation_blocks: 2` this is expected to be rare. A deep reorg that spans a process restart is not detected (retained heads are in memory); if a provider incident makes one plausible, rewind past the incident window deliberately.

## 3. Catch-up too large

**Signal** — the process exits at start with:

```text
ingestion catch-up exceeds max_catchup_blocks: need blocks A-B (N blocks, max M); rewind deliberately or raise ethereum.max_catchup_blocks
```

The gap from the cursor to the tip exceeds the bound; the bound exists so a stale database or an unreviewed rewind does not start a long unattended RPC walk. Raise `ethereum.max_catchup_blocks` above `N`, restart, and lower it again once `/health` shows the head near the tip. The supervisor will otherwise restart the process into the same error.

## 4. Re-running the backfill

The stages are idempotent, so the same command resumes after a failure. To reload one partition deliberately (a corrupt copy, a re-exported directory): stop the service, empty the partition (`TRUNCATE eth_logs_pNNN`), run `-stage logs` (only that directory loads), then `-stage finish` (indexes already exist — tolerated — and the cursor is re-derived from `eth_blocks`). If the indexes were not dropped first the reload is slower but correct.

Never run `backfill` while ingestion is writing: `finish` would set the cursor from `MAX(eth_blocks.number)` under a concurrent writer, and `prepare` would drop the indexes the API is serving from.

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
| info | `Log pagination walk progress` | every 250 windows of a long walk |
| **error** | `ff-eth-logs stopped with error` | The exit line; the wrapped error says why (`new heads subscription error`, `fetch ingestion logs for blocks A-B`, `write blocks A-B`, catch-up bound) |
| info | `JSON-RPC server listening` | `addr` |
| info | `Tail ingestion disabled; serving the stored head only` | `ingestion_enabled=false` |

**Sentry** — `sentry_dsn` sends error-level events only (the two error lines above and the exit), with info lines as breadcrumbs; the service is tagged `service=ff-eth-logs`.

**Database** — `SELECT block_number, updated_at FROM ingest_cursor;` shows the head and when it last moved; `pg_total_relation_size('eth_logs')` tracks growth against the ≈ 1.5 GB/month model.

## 7. Deployment via ff-deploy

The service is deployed by ff-deploy (push the config and image bump to `main` in one commit):

- **Image** `${IMAGE}` (built from `tools/docker/Dockerfile`); the container starts the binary with `-config /app/config.yaml`.
- **Config** rendered to the host and mounted at `/app/config.yaml`; non-secret keys come from the template, secrets from the environment.
- **Secrets**: `FF_ETH_LOGS_ETHEREUM_WEBSOCKET_URL` (the Chainstack WebSocket URL with its key — the same endpoint the indexer uses) and `FF_ETH_LOGS_DATABASE_PASSWORD`.
- **PostgreSQL**: its own container and its own volume (≈ 250 GB), initialised from `db/init_pg_db.sql`; excluded from backups — the GCS export is the DR source.
- **Network**: both containers on the backend network only; port 8545 is not published outside it (no auth on the endpoint).
- **Restart policy**: the container must restart on exit — ingestion errors are fatal by design and recovery is "restart from the cursor".
- **Healthcheck**: `GET /health` returns 200 whenever the database answers, including during catch-up.

Deployment order for a schema change: migration first (with ingestion stopped if it touches `eth_logs` indexes), then the image.

## 8. Backfill on the deployed host

The `backfill` subcommand runs inside the same image against the same database — a one-off container with the export copy mounted and `backfill -config /app/config.yaml -dir <mounted export>` as the command — with the `serve` container stopped. Apply the PostgreSQL session settings from section 1.3 before starting.

## 9. Re-exporting from BigQuery

When history must be regenerated (a new signature or shape rule, a corrupt export, or simply a fresher starting point), re-run `warehouse_extract.sql` — the full script and its rationale are in [probe_2026-08.md](probe_2026-08.md):

- One BigQuery script, one pass over `bigquery-public-data.crypto_ethereum.logs` (7.4 B rows): materialise the shaped set into `indexer-eth-logs.eth_warehouse.logs`, export `logs/part=NNN/` and `blocks/` to a new prefix under `gs://eth-logs/eth-log-warehouse/`, compute the sizing stats.
- Cost and time of the 2026-08-28 run: **134 s**, 3,622 GB billed ≈ **$20.6 list, ≈ $14 after the free TiB**; set "Maximum bytes billed" to 4 TB as a hard stop before running.
- Change the `bucket` DECLARE to a new version prefix (`v2`, …) rather than overwriting `v1`; keep the shape rules in the `WHERE` clause identical to `eventset.Keep`, or change both in the same PR.
- Verify with the external-table round-trip (row count and distinct `(block_number, log_index)` equal) before loading.
