{#-
============================================================================
  Privacy macros — Phase 3
============================================================================
  Reusable SQL snippets for k-anonymity enforcement and privacy audit logging.

  The important design rule: public People Analytics marts may show dimensions
  for small cohorts, but they must not show exact metric values for cohorts
  below `var('k_anonymity_threshold')`. That keeps analysts oriented without
  leaking a one-person headcount/attrition fact.
-#}

{% macro k_anonymity_threshold() -%}
{{ var('k_anonymity_threshold', 5) }}
{%- endmacro %}


{% macro is_k_anonymous(cohort_count_expr) -%}
({{ cohort_count_expr }} >= {{ k_anonymity_threshold() }})
{%- endmacro %}


{% macro k_anonymize(metric_expr, cohort_count_expr, data_type='number(38, 6)') -%}
case
    when {{ is_k_anonymous(cohort_count_expr) }} then cast({{ metric_expr }} as {{ data_type }})
    else cast(null as {{ data_type }})
end
{%- endmacro %}


{% macro k_suppression_reason(cohort_count_expr) -%}
case
    when {{ is_k_anonymous(cohort_count_expr) }} then cast(null as varchar)
    else 'K_ANONYMITY_THRESHOLD'
end
{%- endmacro %}


{% macro k_cohort_size_bucket(cohort_count_expr) -%}
case
    when {{ is_k_anonymous(cohort_count_expr) }} then to_varchar({{ cohort_count_expr }})
    else '<' || to_varchar({{ k_anonymity_threshold() }})
end
{%- endmacro %}


{% macro sql_string_literal(value) -%}
'{{ (value | string).replace("'", "''") }}'
{%- endmacro %}


{% macro insert_privacy_audit_event(
    actor,
    query_surface,
    purpose,
    filters_json='{}',
    result_row_count='null',
    suppressed_row_count='null'
) -%}
{#-
  Inserts one access event into the Phase 3 audit table. This is intended for
  later FastAPI/Streamlit code to call via dbt run-operation or to mirror in
  application SQL.

  Example:
    dbt run-operation insert_privacy_audit_event --args '{
      "actor": "demo_hrbp",
      "query_surface": "workforce_headcount_daily",
      "purpose": "dashboard_view",
      "filters_json": "{\"department\":\"SAL\"}",
      "result_row_count": 10,
      "suppressed_row_count": 2
    }'

  The table is modeled as incremental so normal dbt builds do not wipe events.
  Avoid dbt full-refresh against privacy_audit_log in any environment where
  audit history matters.
-#}

{% set audit_relation = target.database ~ "." ~ target.schema ~ "_people_analytics.privacy_audit_log" %}
{% set insert_sql %}
insert into {{ audit_relation }} (
    audit_event_id,
    audited_at,
    actor,
    query_surface,
    purpose,
    filters_json,
    k_anonymity_threshold,
    result_row_count,
    suppressed_row_count,
    privacy_policy_version,
    dbt_invocation_id
)
select
    uuid_string(),
    current_timestamp(),
    {{ sql_string_literal(actor) }},
    {{ sql_string_literal(query_surface) }},
    {{ sql_string_literal(purpose) }},
    try_parse_json({{ sql_string_literal(filters_json) }}),
    {{ k_anonymity_threshold() }},
    {{ result_row_count }},
    {{ suppressed_row_count }},
    'phase_3_k_anonymity_v1',
    '{{ invocation_id }}'
{% endset %}

{% if execute %}
    {% do run_query(insert_sql) %}
{% endif %}

{{ return(insert_sql) }}
{%- endmacro %}
