-- Every HRIS employment spell should appear exactly once in dim_employee.

with hris as (
    select count(*) as spell_count
    from {{ ref('stg_hris__employees') }}
),

dim as (
    select count(*) as spell_count
    from {{ ref('dim_employee') }}
)

select
    hris.spell_count as hris_spell_count,
    dim.spell_count as dim_spell_count
from hris
cross join dim
where hris.spell_count != dim.spell_count
