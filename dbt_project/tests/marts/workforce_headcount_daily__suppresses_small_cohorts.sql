-- Recompute raw daily headcount cohorts from the core fact. Any below-k cohort
-- in the public mart must be marked non-reportable.

with raw_cohorts as (
    select
        snapshot_date,
        coalesce(department, 'UNKNOWN') as department,
        coalesce(location, 'UNKNOWN') as location,
        coalesce(employment_type, 'UNKNOWN') as employment_type,
        count(distinct canonical_person_id) as raw_cohort_count
    from {{ ref('fct_workforce_daily') }}
    where is_active_on_date
    group by
        snapshot_date,
        coalesce(department, 'UNKNOWN'),
        coalesce(location, 'UNKNOWN'),
        coalesce(employment_type, 'UNKNOWN')
)

select public.*
from {{ ref('workforce_headcount_daily') }} public
inner join raw_cohorts raw
    on public.snapshot_date = raw.snapshot_date
   and public.department = raw.department
   and public.location = raw.location
   and public.employment_type = raw.employment_type
where raw.raw_cohort_count < {{ var('k_anonymity_threshold') }}
  and public.is_reportable
