# FF Eth Logs

[![Tests](https://github.com/feral-file/ff-eth-logs/actions/workflows/test.yaml/badge.svg)](https://github.com/feral-file/ff-eth-logs/actions/workflows/test.yaml)
[![Lint](https://github.com/feral-file/ff-eth-logs/actions/workflows/lint.yaml/badge.svg)](https://github.com/feral-file/ff-eth-logs/actions/workflows/lint.yaml)

A PostgreSQL warehouse of Ethereum mainnet NFT event logs, served over a JSON-RPC endpoint that answers `eth_getLogs` exactly as a node would for the stored event set.

## Purpose

`ff-eth-logs` stores every mainnet log the Feral File indexer can consume — ERC-721 `Transfer`, ERC-1155 `TransferSingle` / `TransferBatch` / `URI`, EIP-4906 `MetadataUpdate` / `BatchMetadataUpdate`, and the three CryptoPunks events — from genesis to a few blocks behind the chain tip, and serves them with go-ethereum's own filter semantics:

- **`eth_getLogs` over full history in one query** — no 10k-block span cap, no per-window billing, no pagination walk. An owner scan or a token's provenance is one SQL query instead of ~2,600 vendor calls.
- **Exact results** — the stored set is the shape-filtered set the indexer's parsers accept; for an in-scope filter the answer is tuple-for-tuple what a node returns. Anything the warehouse cannot answer exactly is refused, never partially answered.
- **Kept current** — a tail ingestion job follows `newHeads` with a confirmation lag and writes each confirmed block, its logs and the cursor in one transaction.
- **Rebuildable** — the full history comes from a one-off BigQuery export loaded by the `backfill` command; nothing in the warehouse is unique state.

The consumer is the Feral File indexer ([ff-indexer-v2](https://github.com/feral-file/ff-indexer-v2)): its routing Ethereum client sends historical `eth_getLogs` traffic here instead of to a vendor RPC, and keeps only live-state calls (`eth_call`, `newHeads`, the tip range) on the vendor. Any internal service that needs NFT logs can use the same endpoint.

## Quick Start

### Docker Compose (Recommended)

```bash
git clone https://github.com/feral-file/ff-eth-logs.git
cd ff-eth-logs

# Create config/.env and config/.env.local from the templates
make setup

# Set FF_ETH_LOGS_ETHEREUM_WEBSOCKET_URL in config/.env.local (or copy the
# sample config: cp cmd/ff-eth-logs/config.yaml.sample config/config.yaml)

# Build and start PostgreSQL + ff-eth-logs
make quickstart
```

This starts **PostgreSQL** (host port `5433`, schema from `db/init_pg_db.sql`) and **ff-eth-logs** (JSON-RPC on `http://localhost:8545`). A fresh warehouse is empty and starts ingesting at the chain head; history below it needs the backfill described in [docs/operations.md](docs/operations.md).

**Configuration**: YAML config file plus `FF_ETH_LOGS_*` environment variables; environment values override the file. See [DEVELOPMENT.md](DEVELOPMENT.md).

### Local Development

Run PostgreSQL in Docker and the binary locally:

```bash
make up-infra
go run ./cmd/ff-eth-logs -config cmd/ff-eth-logs/config.yaml.sample
```

The sample config and `config/.env` point at port `5432`; when the database is the compose container put `FF_ETH_LOGS_DATABASE_PORT=5433` in `config/.env.local` (the env files override the shell, see [DEVELOPMENT.md](DEVELOPMENT.md)).

## JSON-RPC surface

- `POST /` — JSON-RPC 2.0: `eth_getLogs`, `eth_blockNumber` (the warehouse head, not the chain tip), `eth_chainId`. Every other method returns `-32601`.
- `GET /health` — `{"status":"ok","head":<block>,"empty":<bool>,"chain_id":1}`.
- A filter must pin `topics[0]` to warehouse signatures and stay at or below the head; otherwise the reply is `-32000 out of warehouse scope: ...`, which a routing client treats as "ask the vendor".

```bash
curl -s -X POST -H 'content-type: application/json' http://localhost:8545 --data '{
  "jsonrpc": "2.0", "id": 1, "method": "eth_getLogs",
  "params": [{
    "fromBlock": "0x0", "toBlock": "latest",
    "address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
    "topics": ["0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8"]
  }]
}'
```

Full contract: [docs/api_design.md](docs/api_design.md).

## Documentation

- **[Agent Guide](AGENTS.md)** - Repository workflow, canonical verification, and PR/review contract
- **[Business Requirements](docs/business_requirements.md)** - Why the warehouse exists, who consumes it, scope and success criteria
- **[Constraints](docs/constraints.md)** - Data, compatibility, operational and security guardrails
- **[API Design Rules](docs/api_design.md)** - The JSON-RPC surface and the `eth_getLogs` parity contract
- **[Architecture](docs/architecture.md)** - Components, tail ingestion, serving path, backfill, data flow
- **[Database Schema](docs/schema.md)** - Tables, partitions, indexes, size model, migrations
- **[Operations](docs/operations.md)** - First deployment, backfill, reorg and catch-up runbooks, monitoring, deployment
- **[BigQuery probe 2026-08](docs/probe_2026-08.md)** - The cost probe, export layout, Parquet schema and the re-runnable extract script
- **[Development Guide](DEVELOPMENT.md)** - Local stack, configuration, running, testing, debugging
- **[Contributing Guide](CONTRIBUTING.md)** - Setup, linting, testing, and PR process

## Components

One binary, `cmd/ff-eth-logs`, with three subcommands: `serve` (default: JSON-RPC API plus tail ingestion in one process), `backfill` (load the Parquet export), `rewind` (drop blocks above a height and move the cursor).

- **`internal/config`** — YAML + `FF_ETH_LOGS_*` configuration, defaults and validation
- **`internal/logger`** — zap structured logging with a `component` field; errors forwarded to Sentry when a DSN is set
- **`internal/eventset`** — the nine event signatures and the topic-shape rules that define what is stored and what is servable
- **`internal/chain`** — the Ethereum RPC client (newHeads, `eth_getLogs`, `eth_getBlockByNumber`, `eth_getBlockReceipts`, `eth_blockNumber`) with the indexer's retry policy, error classification and adaptive pagination
- **`internal/ingestion`** — head-following tail ingestion: confirmation lag, reorg accounting, bounded catch-up, dense-block receipts fallback
- **`internal/logstore`** — the PostgreSQL schema, the atomic range writer, the `eth_getLogs` reader and the cursor
- **`internal/rpcapi`** — the `eth` namespace on go-ethereum's `rpc.Server`, filter parsing copied from geth, `/health`
- **`internal/backfill`** — Parquet export loader (drop indexes, COPY sorted per partition, load blocks, rebuild indexes, set cursor)
- **`internal/mocks`** — generated mocks (`go generate ./...`; never edit by hand)

## Requirements

- Go 1.25+
- PostgreSQL 18
- Docker and Docker Compose
- An Ethereum mainnet WebSocket RPC endpoint (`newHeads` + `eth_getLogs` + `eth_getBlockReceipts`) for tail ingestion

## License

[MPL-2.0](LICENSE).
