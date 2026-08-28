# Contributing

Thank you for your interest in contributing to FF Eth Logs! This document outlines the setup process, development workflow, and PR guidelines.

## Setup

### Prerequisites

- Go 1.25.0 or later
- Docker and Docker Compose (local PostgreSQL and testcontainers-backed integration tests)
- PostgreSQL 18+ (for local development)
- `golangci-lint` v2.4.0
- `goimports` (`go install golang.org/x/tools/cmd/goimports@latest`)
- Access to an Ethereum mainnet RPC endpoint (HTTP for catch-up, WebSocket for `newHeads`)
- Git

### Initial Setup

1. **Fork and clone the repository**:
   ```bash
   git clone https://github.com/feral-file/ff-eth-logs.git
   cd ff-eth-logs
   ```

2. **Configure your environment** (choose one or both):

   **Option A: Environment Variables**:
   ```bash
   make setup
   ```
   This creates `config/.env` and `config/.env.local` from templates.
   Edit `config/.env.local` with your local settings.

   **Option B: YAML Config File**:
   ```bash
   cp cmd/ff-eth-logs/config.yaml.sample config/config.yaml
   ```
   Edit `config/config.yaml` with your settings.

3. **Required configuration** (in env vars or YAML):
   - Database credentials (PostgreSQL; the log warehouse)
   - Ethereum mainnet RPC URLs (HTTP and WebSocket) for tail ingestion
   - Confirmation lag and HTTP listen address for the JSON-RPC server

   **Note**: Environment variables (with `FF_ETH_LOGS_` prefix) override YAML config values. See [DEVELOPMENT.md](DEVELOPMENT.md) for configuration details.

4. **Start infrastructure**:
   ```bash
   make dev
   ```
   This brings up Docker Compose from [`tools/docker/Docker-compose.yaml`](tools/docker/Docker-compose.yaml) (PostgreSQL for the log warehouse, plus `ff-eth-logs` when using `make up` / `make quickstart`).

5. **Verify setup**:
   - PostgreSQL: `psql -h localhost -U postgres -d ff_eth_logs`
   - After `go run ./cmd/ff-eth-logs serve`, JSON-RPC: `http://localhost:8545` (port from config), e.g. `curl -s -X POST -H 'content-type: application/json' --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' http://localhost:8545`

## Development Workflow

### Running Locally

After starting infrastructure with `make dev`, run the binary:

```bash
# Serve the JSON-RPC endpoint and follow the chain head
go run ./cmd/ff-eth-logs serve -config config/config.yaml

# One-off: load a Parquet export into the warehouse
go run ./cmd/ff-eth-logs backfill -config config/config.yaml -dir ./data/v1

# One-off: drop logs above a block height (reorg or bad-load recovery)
go run ./cmd/ff-eth-logs rewind -config config/config.yaml -to <n>
```

### Code Structure

- `cmd/ff-eth-logs/` - Single binary with `serve`, `backfill`, and `rewind` subcommands
- `internal/` - Internal packages (not exported)
  - `config/` - Configuration loading (`FF_ETH_LOGS_` env prefix, YAML file)
  - `logger/` - Structured logging (`zap`)
  - `eventset/` - Served event signatures and topic shape rules (ERC-721, ERC-1155, EIP-4906, CryptoPunks)
  - `chain/` - Ethereum RPC client adapter (HTTP `eth_getLogs` catch-up, WebSocket `newHeads`)
  - `ingestion/` - Head-driven tail ingestion with confirmation lag
  - `logstore/` - PostgreSQL writer/reader for the log warehouse
  - `rpcapi/` - HTTP JSON-RPC server (`eth_getLogs`, `eth_blockNumber`, `eth_chainId`)
  - `backfill/` - Parquet export loader
  - `mocks/` - Generated mocks (`go generate ./...`; never edit by hand)
- `db/` - Database migrations and schema
- `docs/` - Documentation
- `tools/docker/` - Compose file and image recipe

### Testing

The canonical pre-review verification command is:

```bash
make check
```

This runs, in order: `imports` (`goimports`), `fmt-check` (`gofmt -s -l`), `lint` (`golangci-lint`), `test` (`go test -cover ./...`), and `test-integration` (`go test -tags=integration -cover ./...`). See the `check` target in the `Makefile` for the exact dependency chain.

Integration tests carry the `//go:build integration` tag and need PostgreSQL: they start a testcontainers instance when `TEST_DB_HOST` is unset, or use an external database described by the `TEST_DB_*` environment variables. `-tags=integration` is additive, so `make test-integration` runs unit and integration tests together (same as CI).

The lint profile checks cyclomatic and cognitive complexity, function and file length, and doc quality.

Coverage policy is non-regression versus the base branch. If coverage drops, explain why in the PR body and call out any follow-up work.

For narrower debugging loops, you can still run tests directly:

Run tests:
```bash
go test ./...
```

Run tests with coverage:
```bash
go test -cover ./...
```

Run tests for a specific package:
```bash
go test ./internal/logstore/...
```

Run the integration suite for one package against a local database:
```bash
TEST_DB_HOST=localhost TEST_DB_PORT=5432 TEST_DB_USER=postgres TEST_DB_PASSWORD=postgres TEST_DB_NAME=ff_eth_logs_test \
  go test -tags=integration ./internal/logstore/...
```

### Linting

`make check` is the authoritative lint-and-test gate for substantive changes.

For narrower maintenance work, this project also uses standard Go tooling:

```bash
# Format code
go fmt ./...

# Run vet
go vet ./...

# Check for common issues
docker run --rm -v $(pwd):/app -w /app golangci/golangci-lint:v2.4.0 golangci-lint run --verbose
```

### Building

Build the Docker image:
```bash
make build
```

Build the binary locally:
```bash
go build -o bin/ff-eth-logs ./cmd/ff-eth-logs
```

## Pull Request Process

### Before Submitting

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Follow Go conventions and style
   - Prefer extracting helpers or simplifying control flow before accepting a large function
   - Add tests for new functionality
   - Update documentation as needed
   - Add doc comments to changed Go functions
   - For non-trivial changed functions, use the doc comment to record the reason, trade-offs, and constraints behind the implementation
   - Run `make check`

3. **Commit your changes**:
   - Use clear, descriptive commit messages
   - Reference issue numbers if applicable
   - Follow conventional commits format

4. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

### PR Guidelines

1. **Title**: Clear, descriptive title
2. **Description**: 
   - What changes were made
   - Why the changes were needed
   - How to test the changes
   - Any breaking changes

3. **Link to Issue**: Reference related issues

4. **Checklist**: 
   - [ ] `make check` passes locally, or the blocker is documented
   - [ ] Code is formatted
   - [ ] Documentation updated
   - [ ] No breaking changes (or documented)

### PR Template

When creating a PR, use the template at [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md). Fill out all relevant sections:

- **Problem**: What is changing
- **Why It Matters**: Why the work should land now
- **Acceptance Checks**: 1-3 concrete checks reviewers can use
- **Human Owner**: Who owns the outcome
- **How The Agent Will Be Used**: What the agent did for implementation, review, or follow-up
- **PR or Deploy Link**: The relevant PR, deploy, or release reference

### Code Review

- All PRs require at least one approval
- Address review comments promptly
- Keep PRs focused and reasonably sized
- Rebase on main if needed before merging
- Review the full diff before requesting review and again after addressing feedback
- Rerun `make check` after each substantive review update

## Coding Standards

### Go Style

- Follow [Effective Go](https://go.dev/doc/effective_go) guidelines
- Use `gofmt` for formatting
- Prefer explicit error handling
- Use meaningful variable names
- Keep functions small and focused

### Error Handling

- Always handle errors explicitly
- Use `fmt.Errorf` with `%w` for error wrapping
- Return errors, don't log and ignore
- Map errors to JSON-RPC error responses at the `rpcapi` boundary only; inner packages return plain wrapped errors
- Tail ingestion must surface RPC and database failures explicitly (log and retry with backoff); never skip a block range silently

### Logging

- Use structured logging with `zap`
- Use appropriate log levels (Debug, Info, Warn, Error)
- Include context in log messages (block range, contract address, request id)
- Use `logger.InfoCtx` for context-aware logging

### Testing

- Write unit tests for new functions
- Use table-driven tests when appropriate
- Mock external dependencies (RPC client, store) with the generated mocks in `internal/mocks`
- Test error cases, including reorg and partial-batch paths
- Put integration tests behind `//go:build integration` and keep them beside the package they cover

### Database

- Use transactions for multi-step operations (a block range's logs and its cursor advance commit together)
- Handle connection errors gracefully
- Use prepared statements for queries
- Validate inputs before database operations
- Every schema change updates both `db/migrations/` and `db/init_pg_db.sql`

## Documentation

- Update relevant documentation when making changes
- Add comments for exported functions
- Document complex algorithms or business logic
- Update architecture docs for significant changes

## Questions?

- Open an issue for bugs or feature requests
- Check existing issues and discussions
- Review the codebase and documentation

Thank you for contributing!
