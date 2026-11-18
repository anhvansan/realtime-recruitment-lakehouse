.PHONY: help up down clean nuke-data disk-report s3-check lint test format

.DEFAULT_GOAL := help

help: ## Show available commands and their descriptions
	@echo "Available commands in CPC AdTech Lakehouse:"
	@echo "  make help        - Show this help message"
	@echo "  make lint        - Run ruff linter check"
	@echo "  make format      - Run ruff code formatter"
	@echo "  make test        - Run unit tests with pytest"
	@echo "  make up          - Start core infrastructure services (Task 1.3)"
	@echo "  make down        - Stop running infrastructure services (Task 1.3)"
	@echo "  make clean       - Clean dangling containers and temporary artifacts (Task 1.5)"
	@echo "  make nuke-data   - Reset all data volumes with confirmation (Task 1.5)"
	@echo "  make disk-report - Report disk usage on Drive C & D (Task 1.5)"
	@echo "  make s3-check    - Run MinIO S3 conformance tests (Task 1.2)"

lint: ## Run code linter
	ruff check .

format: ## Format code with ruff
	ruff format .

test: ## Run unit tests
	pytest -q

up: ## Start core services
	@echo "Target 'up' will be configured in Task 1.3 (docker compose)"

down: ## Stop services
	@echo "Target 'down' will be configured in Task 1.3"

clean: ## Clean dangling containers and caches
	@echo "Target 'clean' will be configured in Task 1.5"

nuke-data: ## Wipe lakehouse data
	@echo "Target 'nuke-data' will be configured in Task 1.5"

disk-report: ## Report disk usage
	@echo "Target 'disk-report' will be configured in Task 1.5"

s3-check: ## Verify MinIO S3 API conformance
	@echo "Target 's3-check' will be configured in Task 1.2"
