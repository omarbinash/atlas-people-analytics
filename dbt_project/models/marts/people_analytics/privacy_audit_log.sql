{{
    config(
        materialized='incremental',
        unique_key='audit_event_id',
        on_schema_change='sync_all_columns',
        tags=['marts', 'people_analytics', 'phase_3', 'privacy', 'audit']
    )
}}

-- =============================================================================
-- privacy_audit_log - Phase 3 access audit table
-- =============================================================================
-- Empty-on-build audit table for future FastAPI / Streamlit access events.
-- The `insert_privacy_audit_event` macro inserts rows into this table.
--
-- Why incremental with a zero-row select? A normal dbt build creates and
-- preserves the table shape without erasing existing events. A full-refresh
-- would still recreate the table, so do not full-refresh this model in any
-- environment where audit history matters.
-- =============================================================================

select
    cast(null as varchar(36))                                                as audit_event_id,
    cast(null as timestamp_ntz)                                              as audited_at,
    cast(null as varchar(255))                                               as actor,
    cast(null as varchar(255))                                               as query_surface,
    cast(null as varchar(255))                                               as purpose,
    parse_json(null)                                                         as filters_json,
    cast(null as integer)                                                    as k_anonymity_threshold,
    cast(null as integer)                                                    as result_row_count,
    cast(null as integer)                                                    as suppressed_row_count,
    cast(null as varchar(255))                                               as privacy_policy_version,
    cast(null as varchar(255))                                               as dbt_invocation_id
where false
