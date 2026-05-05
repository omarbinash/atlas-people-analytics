-- Suppressed headcount rows must not expose exact metrics.

select *
from {{ ref('workforce_headcount_daily') }}
where not is_reportable
  and (
      headcount is not null
      or reportable_cohort_employee_count is not null
      or suppression_reason != 'K_ANONYMITY_THRESHOLD'
  )
