.PHONY: help up down clean nuke-data disk-report s3-check lint test format profile status stats

.DEFAULT_GOAL := help

# Detect python executable (prefers python3 on Linux/WSL, python on Windows)
PYTHON := $(shell which python3 2>/dev/null || which python 2>/dev/null || echo python)

help: ## Show available commands and their descriptions
	@echo "Available commands in CPC AdTech Lakehouse:"
	@echo "  make help        - Show this help message"
	@echo "  make up          - Start core infrastructure services (MinIO, Redpanda)"
	@echo "  make down        - Stop running infrastructure services"
	@echo "  make status      - Check status of core services"
	@echo "  make stats       - Inspect container resource usage (RAM/CPU)"
	@echo "  make s3-check    - Run MinIO S3 conformance tests (Task 1.2)"
	@echo "  make profile     - Profile all 18 raw CSV datasets with DuckDB (Task 2.1)"
	@echo "  make lint        - Run ruff linter check"
	@echo "  make format      - Run ruff code formatter"
	@echo "  make test        - Run unit tests with pytest"
	@echo "  make clean       - Clean dangling containers and temporary artifacts (Task 1.5)"
	@echo "  make nuke-data   - Reset all data volumes with confirmation (Task 1.5)"
	@echo "  make disk-report - Report disk usage on Drive C & D (Task 1.5)"

lint: ## Run code linter
	$(PYTHON) -m ruff check .

format: ## Format code with ruff
	$(PYTHON) -m ruff format .

test: ## Run unit tests
	$(PYTHON) -m pytest -q

profile: ## Run CSV profiling script
	$(PYTHON) scripts/profile_tracking.py

up: ## Start core services in background
	docker compose --profile core up -d

down: ## Stop core services
	docker compose --profile core down

status: ## Check core services status
	docker compose --profile core ps

stats: ## View live resource consumption
	docker stats --no-stream

s3-check: ## Verify MinIO S3 API conformance
	$(PYTHON) scripts/s3_conformance.py

clean: ## Clean dangling containers and caches
	@echo "Target 'clean' will be configured in Task 1.5"

nuke-data: ## Wipe lakehouse data
	@echo "Target 'nuke-data' will be configured in Task 1.5"

disk-report: ## Report disk usage
	@echo "Target 'disk-report' will be configured in Task 1.5"
