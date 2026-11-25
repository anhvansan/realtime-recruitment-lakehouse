.PHONY: help up down clean nuke-data disk-report bench-snapshot s3-check topics spark-build spark-up smoke-interop spark-shell lint test format profile status stats

.DEFAULT_GOAL := help

# Detect python executable (prefers python3 on Linux/WSL, python on Windows)
PYTHON := $(shell which python3 2>/dev/null || which python 2>/dev/null || echo python)

help: ## Show available commands and their descriptions
	@echo "Available commands in CPC AdTech Lakehouse:"
	@echo "  make help           - Show this help message"
	@echo "  make up             - Start core infrastructure services (MinIO, Redpanda)"
	@echo "  make down           - Stop running infrastructure services"
	@echo "  make status         - Check status of core & apps services"
	@echo "  make stats          - Inspect container resource usage (RAM/CPU)"
	@echo "  make s3-check       - Run MinIO S3 conformance tests (Task 1.2)"
	@echo "  make topics         - Provision idempotent Redpanda topics (Task 2.4)"
	@echo "  make spark-build    - Build pinned Spark + Delta + DuckDB image (Task 3.1)"
	@echo "  make spark-up       - Start Spark container in profile apps (Task 3.1)"
	@echo "  make smoke-interop  - Run Spark <-> Delta <-> MinIO <-> DuckDB smoke test (Task 3.1)"
	@echo "  make spark-shell    - Open interactive PySpark shell inside Spark container (Task 3.1)"
	@echo "  make profile        - Profile all 18 raw CSV datasets with DuckDB (Task 2.1)"
	@echo "  make lint           - Run ruff linter check"
	@echo "  make format         - Run ruff code formatter"
	@echo "  make test           - Run unit tests with pytest"
	@echo "  make clean          - Clean dangling containers and temporary artifacts (Task 1.5)"
	@echo "  make nuke-data      - Reset all data volumes with confirmation (Task 1.5)"
	@echo "  make disk-report    - Report disk usage on Drive C & D (Task 1.5)"
	@echo "  make bench-snapshot - Record baseline Docker resource usage snapshot (Task 1.6)"

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

down: ## Stop core and apps services
	docker compose --profile core --profile apps down

status: ## Check core and apps services status
	docker compose --profile core --profile apps ps

stats: ## View live resource consumption
	docker stats --no-stream

s3-check: ## Verify MinIO S3 API conformance
	$(PYTHON) scripts/s3_conformance.py

topics: ## Provision idempotent Redpanda topics (Task 2.4)
	@bash infra/redpanda/create_topics.sh

spark-build: ## Build custom Spark + Delta + S3A Docker image (Task 3.1)
	docker compose --profile core --profile apps build spark

spark-up: ## Start Spark container (requires core services healthy)
	docker compose --profile core --profile apps up -d spark

smoke-interop: ## Run Spark <-> Delta <-> MinIO <-> DuckDB interop smoke test (Task 3.1)
	docker compose --profile core --profile apps exec -T spark python3 /app/tests/integration/smoke_delta_interop.py

spark-shell: ## Open interactive PySpark shell in Spark container
	docker compose --profile core --profile apps exec spark /opt/spark/bin/pyspark --master local[2] --conf spark.sql.warehouse.dir=/tmp/spark-warehouse --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

clean: ## Clean dangling containers and caches
	docker compose --profile core down --remove-orphans 2>/dev/null || true
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf .pytest_cache .ruff_cache 2>/dev/null || true
	@echo "✅ Cleaned dangling containers and local caches."

nuke-data: ## Wipe lakehouse data on Drive D (REQUIRES CONFIRMATION 'NUKE')
	@bash -c 'read -r -p "⚠️ CẢNH BÁO: Thao tác này sẽ xoá sạch dữ liệu MinIO và Redpanda trên ổ D! Gõ \"NUKE\" để xác nhận: " confirm; \
	if [ "$$confirm" = "NUKE" ]; then \
		echo "Đang dừng container..."; \
		docker compose --profile core down; \
		DATA_DIR=$$(grep -E "^DATA_ROOT=" .env 2>/dev/null | cut -d "=" -f2- | tr -d "\"" | tr -d "'" || echo "/mnt/d/docker_data/cpc_lakehouse"); \
		DATA_DIR=$${DATA_DIR:-/mnt/d/docker_data/cpc_lakehouse}; \
		echo "Đang dọn sạch dữ liệu tại $$DATA_DIR..."; \
		rm -rf "$$DATA_DIR/minio" "$$DATA_DIR/redpanda"; \
		echo "✅ Đã xoá sạch dữ liệu lakehouse!"; \
	else \
		echo "❌ Đã huỷ thao tác nuke-data."; \
		exit 1; \
	fi'

disk-report: ## Report disk usage on Drive C & D
	@bash scripts/disk_report.sh

bench-snapshot: ## Capture Docker resource baseline snapshot
	@bash scripts/bench/resource_snapshot.sh 12 5 docs/benchmarks/resource_snapshot.csv
