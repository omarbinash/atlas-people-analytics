-- Every row in stg_hris__employees with a non-null DOB and personal_email_local_part
-- must contribute to exactly one int_hris_persons row. This proves the rehire-collapse
-- preserves all input rows (no orphans, no double-counting).
with stg_count as (
    select count(*) as n
    from {{ ref('stg_hris__employees') }}
    where date_of_birth is not null
      and personal_email_local_part is not null
),

persons_spell_sum as (
    select sum(spell_count) as n
    from {{ ref('int_hris_persons') }}
)

select
    stg_count.n  as stg_hris_employees_row_count,
    persons_spell_sum.n  as int_hris_persons_total_spell_count,
    stg_count.n - persons_spell_sum.n as discrepancy
from stg_count
cross join persons_spell_sum
where stg_count.n != persons_spell_sum.n
