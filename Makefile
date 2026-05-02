# ============================================================
# UPI Payments Analytics Pipeline — Makefile
# ============================================================
# Single-command interface for all pipeline operations.
# Run `make help` to see all available commands.

.PHONY: help up down generate load dbt-run dbt-test lint test ci clean

# Default target
help:
	@echo ""
	@echo "UPI Payments Analytics Pipeline"
	@echo "================================"
	@echo "make up          Start all Docker services (MinIO, PostgreSQL, Grafana)"
	@echo "make down        Stop all Docker services"
	@echo "make generate    Generate synthetic UPI transaction data (Parquet → MinIO)"
	@echo "make load        Load Parquet files from MinIO → PostgreSQL raw schema"
	@echo "make dbt-run     Run all dbt models (builds star schema in PostgreSQL)"
	@echo "make dbt-test    Run dbt data quality tests"
	@echo "make dbt-docs    Generate and serve dbt lineage documentation"
	@echo "make lint        Run flake8 linter on all Python files"
	@echo "make test        Run pytest unit tests"
	@echo "make ci          Full CI run: lint + test + dbt-test (mirrors GitHub Actions)"
	@echo "make pipeline    End-to-end: generate + load + dbt-run + dbt-test"
	@echo "make clean       Remove generated data files and dbt artifacts"
	@echo ""

# ----------------------------------------------------------
# Infrastructure
# ----------------------------------------------------------
up:
	@echo "Starting services..."
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 5
	@echo "Services ready:"
	@echo "  MinIO console:  http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  Grafana:        http://localhost:3000  (admin / admin)"
	@echo "  PostgreSQL:     localhost:5432         (upiuser / upipassword)"

down:
	docker-compose down

# ----------------------------------------------------------
# Pipeline steps
# ----------------------------------------------------------

# Step 1: Generate synthetic UPI data and write to MinIO (Bronze)
# generator writes Parquet partitioned by year/month/city
# This is the Bronze layer — raw, unmodified, permanent source of truth
generate:
	@echo "Generating UPI transaction data..."
	python generator/generate_transactions.py
	@echo "Data written to MinIO: upi-lake bucket"

# Step 2: Load from MinIO Bronze → PostgreSQL raw schema (Silver)
# loader reads Parquet from MinIO, cleans, loads to PostgreSQL
# Uses INSERT ON CONFLICT DO NOTHING for idempotency
load:
	@echo "Loading data from MinIO → PostgreSQL raw schema..."
	python loader/load_to_postgres.py
	@echo "Data loaded to PostgreSQL: raw.transactions"

# Step 3: Run dbt transformations (Silver → Gold star schema)
# dbt builds the DAG from ref() calls automatically
# Runs: staging views → intermediate views → mart tables
dbt-run:
	@echo "Running dbt models..."
	cd dbt_project && dbt run --profiles-dir .
	@echo "Star schema built in PostgreSQL marts schema"

# Step 4: Data quality tests
# dbt test runs all schema.yml tests + singular tests
# Any failure returns exit code 1 — blocks CI/CD pipeline
dbt-test:
	@echo "Running dbt data quality tests..."
	cd dbt_project && dbt test --profiles-dir .

dbt-docs:
	@echo "Generating dbt documentation..."
	cd dbt_project && dbt docs generate --profiles-dir .
	cd dbt_project && dbt docs serve --profiles-dir . --port 8080
	@echo "Lineage docs at: http://localhost:8080"

# ----------------------------------------------------------
# Full pipeline — end-to-end in one command
# ----------------------------------------------------------
pipeline: generate load dbt-run dbt-test
	@echo ""
	@echo "Pipeline complete. Open Grafana: http://localhost:3000"

# ----------------------------------------------------------
# Code quality
# ----------------------------------------------------------
lint:
	@echo "Running flake8 linter..."
	flake8 generator/ loader/ tests/ \
		--max-line-length=100 \
		--extend-ignore=E501 \
		--exclude=__pycache__,.venv

test:
	@echo "Running pytest..."
	pytest tests/ -v --tb=short

# Mirrors exactly what GitHub Actions CI runs
ci: lint test dbt-test
	@echo "CI passed."

# ----------------------------------------------------------
# Cleanup
# ----------------------------------------------------------
clean:
	@echo "Cleaning generated artifacts..."
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	rm -rf dbt_project/target dbt_project/logs dbt_project/dbt_packages
	@echo "Clean complete."