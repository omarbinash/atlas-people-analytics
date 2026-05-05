-- Suppressed attrition rows must not expose exact numerator, denominator, or
-- rate values.

select *
from {{ ref('workforce_attrition_monthly') }}
where not is_reportable
  and (
      start_headcount is not null
      or terminations is not null
      or attrition_rate is not null
      or suppression_reason != 'K_ANONYMITY_THRESHOLD'
  )
