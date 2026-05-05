# =============================================================================
# Atlas Makefile — the project's command surface
#
# Run `make help` to see all available targets.
# =============================================================================

.PHONY: help install snowflake-init seed build test lint format clean dashboard api dag-test all

PYTHON := python
PIP := pip
DBT_DIR := dbt_project

# Default target
help:
	@echo "Atlas — People Analytics Foundation"
	@echo ""
	@echo "Setup:"
	@echo "  install          Install Python dependencies (run inside a venv)"
	@echo "  snowflake-init   Provision Snowflake objects (one-time, requires .env)"
	@echo ""
	@echo "Pipeline:"
	@echo "  seed             Generate synthetic data and load to RAW schema"
	@echo "  build            Run dbt build (deps + run + test)"
	@echo "  test             Run dbt tests + Python tests"
	@echo "  all              seed + build + test"
	@echo ""
	@echo "Quality:"
	@echo "  lint             Run ruff and mypy"
	@echo "  format           Auto-format code with ruff"
	@echo ""
	@echo "Local serving:"
	@echo "  dashboard        Launch the Streamlit HRBP dashboard"
	@echo "  api              Launch the FastAPI metrics service"
	@echo "  dag-test         Syntax-check the Airflow DAG"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean            Remove build artifacts and caches"

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
install:
	$(PIP) install -e ".[dev]"
	cd $(DBT_DIR) && dbt deps

snowflake-init:
	@echo "Provisioning Snowflake objects (database, warehouse, role, schemas)..."
	@bash scripts/snowflake_init.sh

# -----------------------------------------------------------------------------
# Pipeline
# -----------------------------------------------------------------------------
seed:
	$(PYTHON) -m seeds.synthesize

build:
	cd $(DBT_DIR) && dbt build --target dev

test:
	cd $(DBT_DIR) && dbt test --target dev
	pytest tests/

all: seed build test

# -----------------------------------------------------------------------------
# Quality
# -----------------------------------------------------------------------------
lint:
	ruff check .
	mypy --config-file pyproject.toml seeds identity_engine api dashboard

format:
	ruff check --fix .
	ruff format .

# -----------------------------------------------------------------------------
# Local serving
# -----------------------------------------------------------------------------
dashboard:
	streamlit run dashboard/app.py --server.port $${ATLAS_DASHBOARD_PORT:-8501}

api:
	uvicorn api.metrics_service:app --reload --host $${ATLAS_API_HOST:-127.0.0.1} --port $${ATLAS_API_PORT:-8000}

dag-test:
	$(PYTHON) -m py_compile airflow/dags/atlas_people_analytics.py

# -----------------------------------------------------------------------------
# Maintenance
# -----------------------------------------------------------------------------
clean:
	rm -rf build/ dist/ *.egg-info
	rm -rf .pytest_cache/ .mypy_cache/ .ruff_cache/ .coverage htmlcov/
	rm -rf $(DBT_DIR)/target/ $(DBT_DIR)/dbt_packages/ $(DBT_DIR)/logs/
	rm -rf seeds/output/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
