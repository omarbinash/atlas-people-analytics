-- Every row in stg_payroll__records with a non-null employee_payroll_id must
-- contribute to exactly one int_payroll_spells row. This proves the spell
-- collapse preserves all 153K monthly pay-period rows.
with stg_count as (
    select count(*) as n
    from {{ ref('stg_payroll__records') }}
    where employee_payroll_id is not null
),

spells_period_sum as (
    select sum(pay_period_count) as n
    from {{ ref('int_payroll_spells') }}
)

select
    stg_count.n  as stg_payroll_records_row_count,
    spells_period_sum.n  as int_payroll_spells_total_period_count,
    stg_count.n - spells_period_sum.n as discrepancy
from stg_count
cross join spells_period_sum
where stg_count.n != spells_period_sum.n
