"""Runtime configuration for the Atlas metrics service."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

from dotenv import find_dotenv, load_dotenv

_SNOWFLAKE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def normalize_snowflake_identifier(value: str, label: str) -> str:
    """Return a safe unquoted Snowflake identifier.

    The metrics API reads database/schema names from environment variables so
    local demos can target different dbt schemas. We still keep table SQL
    deterministic and identifier-safe because these values are interpolated
    into fully qualified relation names rather than passed as query parameters.
    """

    cleaned = value.strip()
    if not _SNOWFLAKE_IDENTIFIER_RE.match(cleaned):
        raise ValueError(f"{label} must be a simple Snowflake identifier")
    return cleaned.upper()


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


@dataclass(frozen=True)
class AtlasSettings:
    """Environment-backed settings shared by the API and dashboard tests."""

    snowflake_account: str | None
    snowflake_user: str | None
    snowflake_password: str | None
    snowflake_role: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_region: str | None
    dbt_schema: str
    people_analytics_schema: str
    api_host: str
    api_port: int
    k_anonymity_min: int
    privacy_policy_version: str = "phase_3_k_anonymity_v1"

    @classmethod
    def from_env(cls) -> AtlasSettings:
        dotenv_path = find_dotenv(usecwd=True)
        if dotenv_path:
            load_dotenv(dotenv_path, override=False)

        dbt_schema = os.getenv("SNOWFLAKE_DBT_SCHEMA", "DBT_DEV").strip()
        people_schema = os.getenv("ATLAS_PEOPLE_ANALYTICS_SCHEMA")
        if not people_schema:
            people_schema = f"{dbt_schema}_PEOPLE_ANALYTICS"

        return cls(
            snowflake_account=os.getenv("SNOWFLAKE_ACCOUNT"),
            snowflake_user=os.getenv("SNOWFLAKE_USER"),
            snowflake_password=os.getenv("SNOWFLAKE_PASSWORD"),
            snowflake_role=os.getenv("SNOWFLAKE_ROLE", "ATLAS_DEVELOPER"),
            snowflake_warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ATLAS_WH"),
            snowflake_database=os.getenv("SNOWFLAKE_DATABASE", "ATLAS"),
            snowflake_schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
            snowflake_region=os.getenv("SNOWFLAKE_REGION"),
            dbt_schema=dbt_schema,
            people_analytics_schema=people_schema,
            api_host=os.getenv("ATLAS_API_HOST", "127.0.0.1"),
            api_port=_env_int("ATLAS_API_PORT", 8000),
            k_anonymity_min=_env_int("ATLAS_K_ANONYMITY_MIN", 5),
        )

    @property
    def database_identifier(self) -> str:
        return normalize_snowflake_identifier(self.snowflake_database, "SNOWFLAKE_DATABASE")

    @property
    def people_analytics_schema_identifier(self) -> str:
        return normalize_snowflake_identifier(
            self.people_analytics_schema,
            "ATLAS_PEOPLE_ANALYTICS_SCHEMA",
        )

    def public_table(self, table_name: str) -> str:
        table_identifier = normalize_snowflake_identifier(table_name, "table_name")
        return (
            f"{self.database_identifier}."
            f"{self.people_analytics_schema_identifier}."
            f"{table_identifier}"
        )

    def missing_connection_values(self) -> list[str]:
        required = {
            "SNOWFLAKE_ACCOUNT": self.snowflake_account,
            "SNOWFLAKE_USER": self.snowflake_user,
            "SNOWFLAKE_PASSWORD": self.snowflake_password,
            "SNOWFLAKE_ROLE": self.snowflake_role,
            "SNOWFLAKE_WAREHOUSE": self.snowflake_warehouse,
            "SNOWFLAKE_DATABASE": self.snowflake_database,
        }
        return [name for name, value in required.items() if not value]

    def snowflake_connect_kwargs(self) -> dict[str, Any]:
        missing = self.missing_connection_values()
        if missing:
            joined = ", ".join(missing)
            raise RuntimeError(f"Missing Snowflake connection settings: {joined}")

        kwargs = {
            "account": self.snowflake_account or "",
            "user": self.snowflake_user or "",
            "password": self.snowflake_password or "",
            "role": self.snowflake_role,
            "warehouse": self.snowflake_warehouse,
            "database": self.snowflake_database,
            "schema": self.people_analytics_schema,
            "client_session_keep_alive": False,
        }
        return kwargs
