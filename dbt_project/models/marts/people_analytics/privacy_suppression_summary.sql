{{
    config(
        materialized='table',
        tags=['marts', 'people_analytics', 'phase_3', 'privacy']
    )
}}

-- =============================================================================
-- privacy_suppression_summary — Phase 3 privacy observability
-- =============================================================================
-- Summarizes how often each public People Analytics mart suppresses metrics.
-- This gives reviewers a quick way to see whether k-anonymity is doing real
-- work and whether a dimension design is creating too many tiny cohorts.
-- =============================================================================

with headcount as (
    select
        'workforce_headcount_daily'                                          as privacy_surface,
        'daily'                                                              as date_grain,
        count(*)                                                             as row_count,
        count_if(is_reportable)                                              as reportable_row_count,
        count_if(not is_reportable)                                          as suppressed_row_count,
        min(k_anonymity_threshold)                                           as k_anonymity_threshold
    from {{ ref('workforce_headcount_daily') }}
),

attrition as (
    select
        'workforce_attrition_monthly'                                        as privacy_surface,
        'monthly'                                                            as date_grain,
        count(*)                                                             as row_count,
        count_if(is_reportable)                                              as reportable_row_count,
        count_if(not is_reportable)                                          as suppressed_row_count,
        min(k_anonymity_threshold)                                           as k_anonymity_threshold
    from {{ ref('workforce_attrition_monthly') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'privacy_surface',
        'date_grain'
    ]) }}                                                                    as privacy_suppression_summary_key,
    privacy_surface,
    date_grain,
    row_count,
    reportable_row_count,
    suppressed_row_count,
    suppressed_row_count::float / nullif(row_count, 0)                       as suppressed_row_rate,
    k_anonymity_threshold,
    current_timestamp()                                                      as generated_at,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from headcount

union all

select
    {{ dbt_utils.generate_surrogate_key([
        'privacy_surface',
        'date_grain'
    ]) }}                                                                    as privacy_suppression_summary_key,
    privacy_surface,
    date_grain,
    row_count,
    reportable_row_count,
    suppressed_row_count,
    suppressed_row_count::float / nullif(row_count, 0)                       as suppressed_row_rate,
    k_anonymity_threshold,
    current_timestamp()                                                      as generated_at,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from attrition
