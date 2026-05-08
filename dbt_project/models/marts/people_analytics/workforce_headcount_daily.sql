{{
    config(
        materialized='table',
        tags=['marts', 'people_analytics', 'phase_3', 'privacy']
    )
}}

-- =============================================================================
-- workforce_headcount_daily - privacy-preserving daily headcount
-- =============================================================================
-- Public People Analytics mart for daily active headcount by common HRBP
-- dimensions. Exact metrics are suppressed when the cohort size is below
-- `var('k_anonymity_threshold')`.
--
-- Tradeoff: dimension rows remain visible even when metrics are suppressed.
-- That helps analysts understand that a cohort exists, but the exact count is
-- hidden. This is more useful than dropping the row entirely and safer than
-- returning small exact counts.
-- =============================================================================

with active_employee_days as (
    select
        snapshot_date,
        coalesce(department, 'UNKNOWN')                                      as department,
        coalesce(location, 'UNKNOWN')                                        as location,
        coalesce(employment_type, 'UNKNOWN')                                 as employment_type,
        canonical_person_id
    from {{ ref('fct_workforce_daily') }}
    where is_active_on_date
),

cohorts as (
    select
        snapshot_date,
        department,
        location,
        employment_type,
        count(distinct canonical_person_id)                                  as cohort_employee_count
    from active_employee_days
    group by
        snapshot_date,
        department,
        location,
        employment_type
)

select
    {{ dbt_utils.generate_surrogate_key([
        'snapshot_date',
        'department',
        'location',
        'employment_type'
    ]) }}                                                                    as headcount_daily_key,
    snapshot_date,
    department,
    location,
    employment_type,
    {{ k_anonymize('cohort_employee_count', 'cohort_employee_count', 'number(38, 0)') }}
                                                                               as headcount,
    {{ k_anonymize('cohort_employee_count', 'cohort_employee_count', 'number(38, 0)') }}
                                                                               as reportable_cohort_employee_count,
    {{ k_cohort_size_bucket('cohort_employee_count') }}                       as cohort_size_bucket,
    {{ is_k_anonymous('cohort_employee_count') }}                             as is_reportable,
    {{ k_suppression_reason('cohort_employee_count') }}                       as suppression_reason,
    {{ k_anonymity_threshold() }}                                             as k_anonymity_threshold,
    '{{ invocation_id }}'                                                     as _dbt_invocation_id
from cohorts
