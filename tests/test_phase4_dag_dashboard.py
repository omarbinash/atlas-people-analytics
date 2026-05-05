from __future__ import annotations

import importlib.util
from pathlib import Path

from dashboard.app import records_frame, suppression_rate


def test_airflow_dag_module_imports_without_airflow_installed() -> None:
    dag_path = Path("airflow/dags/atlas_people_analytics.py")
    spec = importlib.util.spec_from_file_location("atlas_people_analytics_dag", dag_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert hasattr(module, "dag")
    if module.dag is not None:
        assert module.dag.dag_id == "atlas_people_analytics"
        assert {task.task_id for task in module.dag.tasks} == {
            "dbt_deps",
            "build_staging",
            "build_identity_resolution",
            "build_core_marts",
            "build_privacy_marts",
        }


def test_dashboard_records_frame_uses_api_data_key() -> None:
    frame = records_frame({"data": [{"department": "Engineering", "headcount": 12}]})

    assert list(frame.columns) == ["department", "headcount"]
    assert frame.iloc[0].to_dict() == {"department": "Engineering", "headcount": 12}


def test_dashboard_suppression_rate_handles_empty_payload() -> None:
    assert suppression_rate({"row_count": 0, "suppressed_row_count": 10}) == 0.0
    assert suppression_rate({"row_count": 10, "suppressed_row_count": 2}) == 0.2
