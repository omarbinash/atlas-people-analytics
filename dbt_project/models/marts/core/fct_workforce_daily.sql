{{
    config(
        materialized='table',
        tags=['marts', 'core', 'phase_2d', 'fct_workforce_daily']
    )
}}

-- =============================================================================
-- fct_workforce_daily - Phase 2D daily point-in-time workforce snapshot
-- =============================================================================
-- One row per employee spell per calendar date from hire date through either:
--
--   * termination_date, for closed spells
--   * snapshot_as_of_date/current_date, for open spells
--
-- The termination date row is retained with is_active_on_date = false so
-- attrition counts can be computed from the same fact without a separate event
-- table. Headcount metrics should filter is_active_on_date = true.
--
-- Date convention: termination_date is treated as the first non-active day.
-- This matches common HRIS exports where the term date is the effective date of
-- termination rather than the last active workday.
-- =============================================================================

{% if var('snapshot_as_of_date') %}
    {% set snapshot_as_of_expr = "to_date('" ~ var('snapshot_as_of_date') ~ "')" %}
{% else %}
    {% set snapshot_as_of_expr = "current_date()" %}
{% endif %}

with dim_employee as (
    select * from {{ ref('dim_employee') }}
),

date_bounds as (
    select
        min(effective_start_date)                                            as min_snapshot_date,
        {{ snapshot_as_of_expr }}                                            as max_snapshot_date
    from dim_employee
),

date_offsets as (
    select seq4() as day_offset
    from table(generator(rowcount => 10000))
),

date_spine as (
    select
        dateadd(day, date_offsets.day_offset, date_bounds.min_snapshot_date) as snapshot_date
    from date_bounds
    cross join date_offsets
    where dateadd(day, date_offsets.day_offset, date_bounds.min_snapshot_date)
        <= date_bounds.max_snapshot_date
),

employee_days as (
    select
        date_spine.snapshot_date,
        dim_employee.employee_sk,
        dim_employee.canonical_person_id,
        dim_employee.hris_person_key,
        dim_employee.hris_employee_id,
        dim_employee.employment_spell_number,

        dim_employee.effective_start_date,
        dim_employee.effective_end_date,
        dim_employee.termination_date,

        dim_employee.legal_first_name_original,
        dim_employee.legal_last_name_original,
        dim_employee.preferred_name_original,
        dim_employee.employment_type,
        dim_employee.department,
        dim_employee.job_title,
        dim_employee.manager_hris_id,
        dim_employee.location,

        date_spine.snapshot_date >= dim_employee.effective_start_date
            and (
                dim_employee.termination_date is null
                or date_spine.snapshot_date < dim_employee.termination_date
            )                                                                as is_active_on_date,
        date_spine.snapshot_date = dim_employee.effective_start_date          as is_hire_date,
        dim_employee.termination_date is not null
            and date_spine.snapshot_date = dim_employee.termination_date      as is_termination_date,
        datediff(day, dim_employee.effective_start_date, date_spine.snapshot_date)
                                                                               as tenure_days,

        dim_employee.has_rehires,
        dim_employee.has_name_change_marriage,
        dim_employee.matched_external_source_system_count,
        dim_employee.loaded_at
    from date_spine
    inner join dim_employee
        on date_spine.snapshot_date >= dim_employee.effective_start_date
       and date_spine.snapshot_date <= least(
            coalesce(dim_employee.termination_date, date_spine.snapshot_date),
            (select max_snapshot_date from date_bounds)
       )
)

select
    {{ dbt_utils.generate_surrogate_key([
        'snapshot_date',
        'employee_sk'
    ]) }}                                                                    as daily_workforce_key,
    employee_days.*,
    1                                                                        as employee_day_count,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from employee_days
