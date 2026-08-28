.PHONY: help build rebuild run quickstart up down start stop restart logs logs-component ps clean config \
	up-infra up-app dev setup check-env health \
	imports fmt fmt-check lint test test-integration mocks check

# Docker Compose settings
# Use a clean environment so .env files take precedence over host shell variables.
DOCKER_COMPOSE := env -i \
	PATH="$$PATH" \
	HOME="$$HOME" \
	USER="$$USER" \
	docker compose -f tools/docker/Docker-compose.yaml \
	--env-file config/.env \
	--env-file config/.env.local
DOCKER_COMPOSE_UP := $(DOCKER_COMPOSE) up -d --remove-orphans

INFRA_SERVICES := postgres
APP_SERVICE := ff-eth-logs
MODULE := github.com/feral-file/ff-eth-logs

COLOR_RESET := \033[0m
COLOR_BOLD := \033[1m
COLOR_GREEN := \033[32m
COLOR_YELLOW := \033[33m
COLOR_BLUE := \033[34m

##@ General

help: ## Display this help message
	@echo "$(COLOR_BOLD)FF Eth Logs - Make Commands$(COLOR_RESET)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make $(COLOR_BLUE)<target>$(COLOR_RESET)\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  $(COLOR_BLUE)%-25s$(COLOR_RESET) %s\n", $$1, $$2 } /^##@/ { printf "\n$(COLOR_BOLD)%s$(COLOR_RESET)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Build Commands

build: ## Build the app image
	@echo "$(COLOR_BLUE)Building $(APP_SERVICE) image...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build $(APP_SERVICE)
	@echo "$(COLOR_GREEN)✓ Image built$(COLOR_RESET)"

rebuild: ## Rebuild the app image without cache
	@$(DOCKER_COMPOSE) build --no-cache $(APP_SERVICE)

##@ Run Commands

up: ## Start all services
	@$(DOCKER_COMPOSE_UP)
	@$(MAKE) ps

up-infra: ## Start PostgreSQL only
	@$(DOCKER_COMPOSE_UP) $(INFRA_SERVICES)

up-app: up-infra ## Start the app (infra first)
	@$(DOCKER_COMPOSE_UP) $(APP_SERVICE)

start: up ## Alias for up

run: build up ## Build and start everything

quickstart: setup build up ## First run: env files, image, services
	@echo ""
	@echo "$(COLOR_GREEN)╔══════════════════════════════════════════════╗$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  ff-eth-logs is up                            ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  JSON-RPC : http://localhost:8545             ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Health   : http://localhost:8545/health      ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)╚══════════════════════════════════════════════╝$(COLOR_RESET)"

dev: up-infra ## Start infra and print the local run command
	@echo "$(COLOR_YELLOW)Run the app locally with:$(COLOR_RESET)"
	@echo "  go run ./cmd/ff-eth-logs -config cmd/ff-eth-logs/config.yaml.sample"

##@ Control Commands

down: ## Stop and remove services
	@$(DOCKER_COMPOSE) down --remove-orphans

stop: ## Stop services
	@$(DOCKER_COMPOSE) stop

restart: ## Restart services
	@$(DOCKER_COMPOSE) restart

##@ Monitoring Commands

logs: ## Tail all logs
	@$(DOCKER_COMPOSE) logs -f

logs-component: ## Tail app logs for one component (COMPONENT=http-server|ingestion|backfill)
	@$(DOCKER_COMPOSE) logs -f --no-log-prefix $(APP_SERVICE) 2>&1 | grep -E '"component":[[:space:]]*"$(COMPONENT)"'

ps: ## Show service status
	@$(DOCKER_COMPOSE) ps

health: ## Show service health
	@$(DOCKER_COMPOSE) ps --format "table {{.Name}}\t{{.Status}}\t{{.Health}}"

##@ Development Commands

config: ## Show the resolved compose configuration
	@$(DOCKER_COMPOSE) config

mocks: ## Regenerate gomock mocks (go generate ./...)
	@go generate ./...
	@echo "$(COLOR_GREEN)✓ Mocks regenerated$(COLOR_RESET)"

##@ Cleanup Commands

clean: down ## Stop services and remove volumes
	@$(DOCKER_COMPOSE) down -v

##@ Setup Commands

setup: ## Create config/.env.local from the template if missing
	@if [ ! -f config/.env.local ]; then \
		cp config/.env config/.env.local; \
		echo "$(COLOR_YELLOW)Created config/.env.local — set FF_ETH_LOGS_ETHEREUM_WEBSOCKET_URL$(COLOR_RESET)"; \
	else \
		echo "$(COLOR_GREEN)config/.env.local already exists$(COLOR_RESET)"; \
	fi

check-env: ## Verify env files exist
	@test -f config/.env || (echo "config/.env missing" && exit 1)
	@test -f config/.env.local || (echo "config/.env.local missing — run make setup" && exit 1)

##@ Check Commands

imports: ## Format imports
	@goimports -w -local "$(MODULE)" .
	@echo "$(COLOR_GREEN)✓ Imports formatted$(COLOR_RESET)"

fmt: ## Apply gofmt -s formatting (matches CI go fmt check)
	@gofmt -s -w .
	@echo "$(COLOR_GREEN)✓ Go code formatted$(COLOR_RESET)"

fmt-check: ## Verify gofmt -s formatting (matches CI go fmt check)
	@if [ "$$(gofmt -s -l . | wc -l | tr -d ' ')" -gt 0 ]; then \
		echo "$(COLOR_YELLOW)Code is not formatted. Run 'make fmt' or 'gofmt -s -w .'$(COLOR_RESET)"; \
		gofmt -s -l .; \
		exit 1; \
	fi
	@echo "$(COLOR_GREEN)✓ gofmt check passed$(COLOR_RESET)"

lint: ## Run golangci-lint (same version as CI)
	@golangci-lint run --verbose
	@echo "$(COLOR_GREEN)✓ Linters passed$(COLOR_RESET)"

test: ## Run unit tests (no external dependencies)
	@go test -cover ./...
	@echo "$(COLOR_GREEN)✓ Unit tests passed$(COLOR_RESET)"

test-integration: ## Run unit + integration tests (needs Docker or TEST_DB_* env vars)
	@go test -tags=integration -cover ./...
	@echo "$(COLOR_GREEN)✓ Integration tests passed$(COLOR_RESET)"

check: imports fmt-check lint test test-integration ## Format imports, verify gofmt -s, lint, unit tests, integration tests
	@echo "$(COLOR_GREEN)✓ All checks passed$(COLOR_RESET)"
