"""Small Snowflake access wrapper for the Atlas metrics API."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from api.settings import AtlasSettings

QueryParams = tuple[Any, ...]


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


class AtlasSnowflakeClient:
    """Executes parameterized queries against Snowflake.

    The service intentionally keeps this wrapper small: Phase 4 is an
    operational facade over dbt-built privacy marts, not a second transformation
    layer with its own business logic.
    """

    def __init__(self, settings: AtlasSettings):
        self._settings = settings

    def _connect(self):
        import snowflake.connector

        return snowflake.connector.connect(**self._settings.snowflake_connect_kwargs())

    def fetch_all(self, sql: str, params: QueryParams = ()) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(sql, params)
                columns = [column[0].lower() for column in cursor.description or []]
                return [
                    {column: _json_ready(value) for column, value in zip(columns, row, strict=True)}
                    for row in cursor.fetchall()
                ]
            finally:
                cursor.close()
        finally:
            connection.close()

    def execute(self, sql: str, params: QueryParams = ()) -> None:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(sql, params)
            finally:
                cursor.close()
        finally:
            connection.close()
