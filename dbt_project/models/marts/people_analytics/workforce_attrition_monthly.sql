{{
    config(
        materialized='table',
        tags=['marts', 'people_analytics', 'phase_3', 'privacy']
    )
}}

-- =============================================================================
-- workforce_attrition_monthly — privacy-preserving monthly attrition
-- =============================================================================
-- Public monthly attrition mart by HRBP dimensions. The privacy cohort is the
-- active population at the start of the month for that dimension combination.
-- If that population is smaller than k, start headcount, terminations, and rate
-- are suppressed together.
--
-- Suppressing by denominator rather than termination count avoids hiding every
-- month with one departure in a large cohort while still protecting genuinely
-- small teams/locations.
-- =============================================================================

with workforce_daily as (
    select * from {{ ref('fct_workforce_daily') }}
),

month_dimension_spine as (
    select distinct
        date_trunc('month', snapshot_date)::date                              as month_start_date,
        coalesce(department, 'UNKNOWN')                                      as department,
        coalesce(location, 'UNKNOWN')                                        as location,
        coalesce(employment_type, 'UNKNOWN')                                 as employment_type
    from workforce_daily
),

month_first_snapshot as (
    select
        date_trunc('month', snapshot_date)::date                              as month_start_date,
        min(snapshot_date)                                                    as first_snapshot_date
    from workforce_daily
    group by date_trunc('month', snapshot_date)::date
),

month_start_population as (
    select
        date_trunc('month', snapshot_date)::date                              as month_start_date,
        coalesce(department, 'UNKNOWN')                                      as department,
        coalesce(location, 'UNKNOWN')                                        as location,
        coalesce(employment_type, 'UNKNOWN')                                 as employment_type,
        count(distinct canonical_person_id)                                  as start_cohort_employee_count
    from workforce_daily
    inner join month_first_snapshot
        on date_trunc('month', workforce_daily.snapshot_date)::date = month_first_snapshot.month_start_date
       and workforce_daily.snapshot_date = month_first_snapshot.first_snapshot_date
    where is_active_on_date
    group by
        date_trunc('month', snapshot_date)::date,
        coalesce(department, 'UNKNOWN'),
        coalesce(location, 'UNKNOWN'),
        coalesce(employment_type, 'UNKNOWN')
),

terminations as (
    select
        date_trunc('month', snapshot_date)::date                              as month_start_date,
        coalesce(department, 'UNKNOWN')                                      as department,
        coalesce(location, 'UNKNOWN')                                        as location,
        coalesce(employment_type, 'UNKNOWN')                                 as employment_type,
        count(distinct canonical_person_id)                                  as termination_count
    from workforce_daily
    where is_termination_date
    group by
        date_trunc('month', snapshot_date)::date,
        coalesce(department, 'UNKNOWN'),
        coalesce(location, 'UNKNOWN'),
        coalesce(employment_type, 'UNKNOWN')
),

cohorts as (
    select
        spine.month_start_date,
        last_day(spine.month_start_date)                                     as month_end_date,
        spine.department,
        spine.location,
        spine.employment_type,
        coalesce(pop.start_cohort_employee_count, 0)                         as start_cohort_employee_count,
        coalesce(term.termination_count, 0)                                  as termination_count,
        coalesce(term.termination_count, 0)::float
            / nullif(coalesce(pop.start_cohort_employee_count, 0), 0)         as attrition_rate_raw
    from month_dimension_spine spine
    left join month_start_population pop
        on spine.month_start_date = pop.month_start_date
       and spine.department = pop.department
       and spine.location = pop.location
       and spine.employment_type = pop.employment_type
    left join terminations term
        on spine.month_start_date = term.month_start_date
       and spine.department = term.department
       and spine.location = term.location
       and spine.employment_type = term.employment_type
)

select
    {{ dbt_utils.generate_surrogate_key([
        'month_start_date',
        'department',
        'location',
        'employment_type'
    ]) }}                                                                    as attrition_monthly_key,
    month_start_date,
    month_end_date,
    department,
    location,
    employment_type,
    {{ k_anonymize('start_cohort_employee_count', 'start_cohort_employee_count', 'number(38, 0)') }}
                                                                               as start_headcount,
    {{ k_anonymize('termination_count', 'start_cohort_employee_count', 'number(38, 0)') }}
                                                                               as terminations,
    {{ k_anonymize('attrition_rate_raw', 'start_cohort_employee_count', 'float') }}
                                                                               as attrition_rate,
    {{ k_cohort_size_bucket('start_cohort_employee_count') }}                  as cohort_size_bucket,
    {{ is_k_anonymous('start_cohort_employee_count') }}                        as is_reportable,
    {{ k_suppression_reason('start_cohort_employee_count') }}                  as suppression_reason,
    {{ k_anonymity_threshold() }}                                              as k_anonymity_threshold,
    '{{ invocation_id }}'                                                      as _dbt_invocation_id
from cohorts
