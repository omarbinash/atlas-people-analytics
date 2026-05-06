-- Recompute month-start cohort sizes from the core fact. Any below-k cohort in
-- the public attrition mart must be marked non-reportable.

with month_first_snapshot as (
    select
        date_trunc('month', snapshot_date)::date as month_start_date,
        min(snapshot_date) as first_snapshot_date
    from {{ ref('fct_workforce_daily') }}
    group by date_trunc('month', snapshot_date)::date
),

raw_cohorts as (
    select
        date_trunc('month', fct.snapshot_date)::date as month_start_date,
        coalesce(fct.department, 'UNKNOWN') as department,
        coalesce(fct.location, 'UNKNOWN') as location,
        coalesce(fct.employment_type, 'UNKNOWN') as employment_type,
        count(distinct fct.canonical_person_id) as raw_start_cohort_count
    from {{ ref('fct_workforce_daily') }} fct
    inner join month_first_snapshot
        on date_trunc('month', fct.snapshot_date)::date = month_first_snapshot.month_start_date
       and fct.snapshot_date = month_first_snapshot.first_snapshot_date
    where fct.is_active_on_date
    group by
        date_trunc('month', fct.snapshot_date)::date,
        coalesce(fct.department, 'UNKNOWN'),
        coalesce(fct.location, 'UNKNOWN'),
        coalesce(fct.employment_type, 'UNKNOWN')
)

select public.*
from {{ ref('workforce_attrition_monthly') }} public
left join raw_cohorts raw
    on public.month_start_date = raw.month_start_date
   and public.department = raw.department
   and public.location = raw.location
   and public.employment_type = raw.employment_type
where coalesce(raw.raw_start_cohort_count, 0) < {{ var('k_anonymity_threshold') }}
  and public.is_reportable
