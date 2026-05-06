-- A canonical person can have multiple employment spells, but those effective
-- date ranges must not overlap. Otherwise a point-in-time query could resolve
-- one person to multiple HRIS spell rows on the same day.

with ordered_spells as (
    select
        canonical_person_id,
        hris_employee_id,
        effective_start_date,
        effective_end_date,
        lead(effective_start_date) over (
            partition by canonical_person_id
            order by effective_start_date, hris_employee_id
        ) as next_effective_start_date
    from {{ ref('dim_employee') }}
)

select *
from ordered_spells
where next_effective_start_date is not null
  and effective_end_date >= next_effective_start_date
