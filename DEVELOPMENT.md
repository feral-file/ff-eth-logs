# Development Guide

This guide covers local development setup, configuration, running the three subcommands, and testing for FF Eth Logs.

## Local Development Stack

### Infrastructure Services

The development stack uses Docker Compose (`tools/docker/docker-compose.yaml`):

- **PostgreSQL 18** — the warehouse database, initialised from `db/init_pg_db.sql`. Published on host port **5433** (`FF_ETH_LOGS_DB_HOST_PORT` in `config/.env`) so it does not collide with an indexer database on 5432.
- **ff-eth-logs** — the API and tail ingestion, JSON-RPC on port **8545**.

### Make targets

| Target | What it does |
| --- | --- |
| `make setup` | Creates `config/.env` and `config/.env.local` from the templates |
| `make up-infra` | Starts PostgreSQL only |
| `make up` | Starts PostgreSQL and ff-eth-logs |
| `make quickstart` | Builds the image and runs `make up` |
| `make down` | Stops the stack |
| `make logs` | Tails container logs |
| `make ps` | Lists the stack's containers |
| `make check` | Canonical verification (see below) |

### Infrastructure Access

```bash
psql -h localhost -p 5433 -U postgres -d ff_eth_logs
# Password: postgres (default)
```

## Configuration

The binary supports **dual configuration**: a YAML file and `FF_ETH_LOGS_*` environment variables, alone or together (`internal/config/config.go`).

**Lookup order**:

1. `-env <dir>` (default `config/`): `.env` is loaded, then `.env.local`. Both are loaded with override semantics (`godotenv.Overload`), so a non-empty value in `config/.env.local` beats the same variable in `config/.env`, which beats the value exported in the shell. Empty values (`FF_ETH_LOGS_SENTRY_DSN=`) are ignored by viper and do not clear a YAML value.
2. `-config <file>`, or without it `config.yaml` searched in `.`, `cmd/ff-eth-logs/`, `config/`. A missing file is not an error; every key then comes from the environment and defaults.
3. Environment variables override the file: dots become underscores, so `ethereum.websocket_url` → `FF_ETH_LOGS_ETHEREUM_WEBSOCKET_URL`.

**Priority** (highest to lowest): environment (after the env files are applied) → YAML → built-in default.

### Keys and defaults

| Key | Default | Notes |
| --- | --- | --- |
| `debug` | `false` | Development logger and debug level |
| `sentry_dsn` | `""` | Errors (and above) go to Sentry when set |
| `server.host` | `0.0.0.0` | |
| `server.port` | `8545` | |
| `server.read_timeout` | `30s` | |
| `server.write_timeout` | `120s` | A 100k-log response takes a while to encode |
| `server.idle_timeout` | `120s` | |
| `database.host` | — | **Required** |
| `database.port` | `5432` | |
| `database.user` | `postgres` | |
| `database.password` | `""` | |
| `database.dbname` | `ff_eth_logs` | |
| `database.sslmode` | `disable` | |
| `database.max_conns` | `16` | pgx pool size |
| `ethereum.websocket_url` | — | **Required when `ingestion_enabled`**; `newHeads` + `eth_getLogs` on one connection |
| `ethereum.chain_id` | `1` | What `eth_chainId` and `/health` report |
| `ethereum.ingestion_enabled` | `true` | `false` serves the stored head only (API-only replica) |
| `ethereum.start_block` | `0` | Non-zero overrides the cursor unconditionally |
| `ethereum.confirmation_blocks` | `2` | Blocks are written this far behind the tip; must be below `max_catchup_blocks` |
| `ethereum.max_catchup_blocks` | `50000` | Largest cursor-to-tip gap walked on start; `0` = unbounded |
| `ethereum.getlogs_span_cap` | `10000` | Provider `eth_getLogs` block-range cap (`toBlock - fromBlock`); `0` = discover by rejection |
| `rpc.max_results` | `100000` | Above this, `eth_getLogs` returns `query returned more than N results`; `0` = unlimited |
| `rpc.query_timeout` | `60s` | Bounds one `eth_getLogs` database query |

Validation fails fast with `missing required config: ...` listing every missing key, and rejects `ethereum.confirmation_blocks` at or above a non-zero `ethereum.max_catchup_blocks` and a negative `rpc.max_results`.

`FF_ETH_LOGS_DB_HOST_PORT` in `config/.env` is not a config key; it is read by Docker Compose for the PostgreSQL host port.

### Required secrets

```bash
# config/.env.local (git ignored)
FF_ETH_LOGS_DATABASE_PASSWORD=postgres
FF_ETH_LOGS_ETHEREUM_WEBSOCKET_URL=wss://ethereum-mainnet.core.chainstack.com/YOUR_KEY
```

The RPC URL carries the provider API key. The client logs only the scheme and host (`chain.endpointForLogs`); never paste the full URL into an issue.

## Running Locally

Every subcommand takes `-config <file>` and `-env <dir>`; a first argument that does not start with `-` selects the subcommand (default `serve`).

### serve

```bash
go run ./cmd/ff-eth-logs -config cmd/ff-eth-logs/config.yaml.sample
# or explicitly
go run ./cmd/ff-eth-logs serve -config config/config.yaml
```

Runs the JSON-RPC server and, unless `ethereum.ingestion_enabled=false`, tail ingestion in the same process. Either subsystem failing stops the other. On first start with an empty database and no `start_block`, ingestion logs `No cursor and no start_block: starting at the chain head; history below it needs a backfill` and begins at the current head.

### backfill

Loads a local copy of the BigQuery Parquet export. Run it with the service stopped (or `ethereum.ingestion_enabled=false`) on an empty warehouse:

```bash
# Full export (≈16 GB: logs/part=000..025 + blocks/)
gcloud storage cp -r gs://eth-logs/eth-log-warehouse/v1 ./data/

go run ./cmd/ff-eth-logs backfill -config config/config.yaml -dir ./data/v1
```

`-stage` selects one stage instead of `all`: `prepare` (drop the secondary indexes), `logs` (COPY each `logs/part=NNN/` into its partition, sorted by `(block_number, log_index)`), `blocks` (COPY `blocks/*.parquet` into `eth_blocks`), `finish` (verify every export directory and block is loaded, recreate the indexes, `ANALYZE`, publish coverage `[oldest, newest]`; refuses and leaves the cursor unset when the load is incomplete). Every stage is idempotent: `logs` skips a partition that already has rows, `blocks` skips when `eth_blocks` is non-empty, so re-running after a failure resumes where it stopped.

For a quick local run use the one-day rehearsal export instead: `gs://eth-logs/eth-log-warehouse/v0-rehearsal` has the same layout with the logs of 2026-08-20 (72,430 logs, blocks 25,792,602–25,799,779) and the complete `blocks/` export. After `finish` the cursor is the newest block in `eth_blocks`, so the warehouse reports head 25,842,829 and answers `[]` for any in-scope range outside that day.

### rewind

```bash
go run ./cmd/ff-eth-logs rewind -config config/config.yaml -to 25842000
```

Deletes every block above `-to` (the last block to keep) and moves the cursor there; the next `serve` re-fetches from `-to + 1`. `-to` is required and the cursor must be above it, otherwise `rewind to N: cursor is not above it (nothing to rewind)`.

## Database Setup

### Initial Schema

The compose stack applies `db/init_pg_db.sql` when the PostgreSQL container first starts. Manually:

```bash
psql -h localhost -p 5433 -U postgres -d ff_eth_logs -f db/init_pg_db.sql
```

The file is idempotent (`IF NOT EXISTS` throughout) and creates `eth_blocks`, `eth_logs` with 40 range partitions (`eth_logs_p000` … `eth_logs_p039`, 1,000,000 blocks each), the four secondary indexes, and `ingest_cursor`.

### Migrations

Migrations live in `db/migrations/` as `NNN.sql` and are mirrored into `db/init_pg_db.sql`; `001.sql` is the initial schema and simply includes the init file. Apply with `psql -f`. Run a migration before deploying the code that depends on it. The four `CREATE INDEX` statements must stay byte-identical to `logstore.SecondaryIndexes` — the backfill drops and recreates them from that list, and `TestSchemaMatchesInit` compares the two.

### Reset Database

```bash
make down
docker volume rm <the stack's postgres volume>   # docker volume ls
make up-infra
```

## Testing

### Test categories

Tests are split by the `integration` build tag:

- **Unit tests** (no tag) — `make test` (`go test -cover ./...`). No external dependencies; the chain client is mocked with `internal/mocks` (regenerate with `go generate ./...`).
- **Integration tests** (`//go:build integration`) — `make test-integration` (`go test -tags=integration -cover ./...`). The `logstore` and `rpcapi` suites need PostgreSQL 18: a testcontainers instance when `TEST_DB_HOST` is unset, or an external database from `TEST_DB_HOST`, `TEST_DB_PORT`, `TEST_DB_USER`, `TEST_DB_PASSWORD`, `TEST_DB_NAME`. They load `db/init_pg_db.sql` and include:
  - the **differential test**: every filter shape (wildcard positions, a 4-position filter against a 1-topic log, address OR, block-hash lookups) evaluated by go-ethereum's in-memory `filterLogs` semantics and by `logstore.FilterLogs` SQL, results compared tuple for tuple;
  - `TestSchemaMatchesInit`: `logstore.SecondaryIndexes` versus the statements in `db/init_pg_db.sql`.

`-tags=integration` is additive: `make test-integration` runs unit and integration tests together, the same as CI.

```bash
export TEST_DB_HOST=localhost TEST_DB_PORT=5433
export TEST_DB_USER=postgres TEST_DB_PASSWORD=postgres TEST_DB_NAME=ff_eth_logs_test
make test-integration
```

### Canonical Verification

```bash
make check
```

Runs, in order: `imports` (`goimports`), `fmt-check` (`gofmt -s -l`), `lint` (`golangci-lint`), `test`, `test-integration`. Run `make fmt` to apply `gofmt -s -w` when `fmt-check` fails. Docker (or `TEST_DB_*`) must be available or the last step fails.

Coverage policy is non-regression versus the base branch; document any necessary drop in the PR.

## Debugging

### Health endpoint

```bash
curl -s http://localhost:8545/health
# {"status":"ok","head":25842829,"coverage_start":0,"empty":false,"chain_id":1}
```

`head` and `coverage_start` are the covered interval (the API answers only inside it). `empty: true` means no cursor row yet (fresh database, backfill not finished). A database failure returns 503 with `{"status":"error","error":"..."}`. The endpoint is 200 during a long catch-up — lag is not a health failure.

### Logs

Structured JSON (zap). The `component` field identifies the subsystem: `http-server`, `ingestion`, `backfill`. With `debug: true` the logger switches to the development encoder and debug level (per-batch `Wrote confirmed blocks` lines, ignored stale heads, permanent RPC errors).

```bash
make logs
```

### Common Issues

**`ingestion catch-up exceeds max_catchup_blocks`** — the gap between the cursor and the tip is larger than `ethereum.max_catchup_blocks` (50,000 blocks ≈ 7 days). The process exits on purpose (the message continues `need blocks A-B (N blocks, max M); rewind deliberately or raise ethereum.max_catchup_blocks`). Raise `ethereum.max_catchup_blocks` above the gap for the run that fills it; the bound is a guard against an unreviewed rewind or a stale database walking the RPC unattended, not a hard limit of the walk.

**`new heads subscription error: ...`** — the WebSocket subscription dropped. There is no in-process reconnect: the process exits and the supervisor (compose `restart`, ff-deploy) starts it again from the cursor. Repeated exits mean the endpoint or the key is bad; the log line `retryable ethereum error encountered` shows the scheme and host only.

**`out of warehouse scope: ...`** from `eth_getLogs` — the filter has no `topics[0]`, names a signature outside the event set, or reaches above the head (`blocks A-B extend above the warehouse head H`). Check `/health` for the head and see [docs/api_design.md](docs/api_design.md) for the scope rule. `warehouse is empty` means the cursor row does not exist yet.

**`query returned more than 100000 results`** — the filter is too wide for `rpc.max_results`; split the block range (the indexer's pagination halves on this message) or raise the limit.

**Database connection errors** — `docker ps`, then check `database.host`/`database.port` (5433 for the compose container from the host, 5432 from inside the network).

## Tips

1. Point `ethereum.start_block` at a recent height for a local run without a backfill, then set it back to `0` so the cursor takes over.
2. Use `ethereum.ingestion_enabled=false` to iterate on the API against a loaded database without an RPC key.
3. Compare answers with a node: the same `eth_getLogs` body sent to the vendor and to `:8545` must return identical arrays for an in-scope filter at or below the head.
4. `psql -c "SELECT * FROM ingest_cursor"` shows the head and when it last moved.
5. Keep PostgreSQL running and restart only the binary during development.

## Next Steps

- Read [Architecture](docs/architecture.md) for tail ingestion and the serving path
- Read [Schema](docs/schema.md) for tables, partitions and the size model
- Read [Operations](docs/operations.md) before the first deployment
