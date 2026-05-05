"""Airflow DAG for the Atlas synthetic People Analytics pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = PROJECT_ROOT / "dbt_project"
ENV_FILE = PROJECT_ROOT / ".env"

DBT_COMMAND_PREFIX = f"""
set -euo pipefail
cd "{DBT_DIR}"
if [ -f "{ENV_FILE}" ]; then
  set -a
  source "{ENV_FILE}"
  set +a
fi
export DBT_PROFILES_DIR="${{DBT_PROFILES_DIR:-{DBT_DIR}}}"
export DBT_TARGET="${{DBT_TARGET:-dev}}"
"""


try:
    from airflow.operators.bash import BashOperator

    from airflow import DAG
except ImportError:  # pragma: no cover - Airflow is an optional local dependency.
    dag = None
else:
    default_args = {
        "owner": "atlas",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id="atlas_people_analytics",
        description="Build Atlas synthetic People Analytics marts from raw data through privacy surfaces.",
        default_args=default_args,
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        max_active_runs=1,
        tags=["atlas", "people_analytics", "synthetic"],
    ) as dag:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"{DBT_COMMAND_PREFIX}\ndbt deps",
        )

        build_staging = BashOperator(
            task_id="build_staging",
            bash_command=f"{DBT_COMMAND_PREFIX}\ndbt build --select staging",
        )

        build_identity = BashOperator(
            task_id="build_identity_resolution",
            bash_command=(
                f"{DBT_COMMAND_PREFIX}\n"
                "dbt build --select +int_canonical_person+ int_stewardship_queue"
            ),
        )

        build_core = BashOperator(
            task_id="build_core_marts",
            bash_command=f"{DBT_COMMAND_PREFIX}\ndbt build --select +dim_employee+ fct_workforce_daily",
        )

        build_privacy = BashOperator(
            task_id="build_privacy_marts",
            bash_command=(
                f"{DBT_COMMAND_PREFIX}\n"
                "dbt build --select +privacy_suppression_summary+ privacy_audit_log "
                "test_privacy_macros privacy__no_direct_employee_identifiers_in_people_analytics"
            ),
        )

        dbt_deps >> build_staging >> build_identity >> build_core >> build_privacy


__all__ = ["dag"]
