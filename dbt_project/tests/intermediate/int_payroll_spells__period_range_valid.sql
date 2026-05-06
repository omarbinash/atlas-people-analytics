-- Every spell's first_pay_period_start must be on or before its
-- most_recent_pay_period_end. A spell with first > most_recent is a
-- collapse logic bug.
select
    payroll_spell_key,
    employee_payroll_id,
    first_pay_period_start,
    most_recent_pay_period_end
from {{ ref('int_payroll_spells') }}
where first_pay_period_start > most_recent_pay_period_end
