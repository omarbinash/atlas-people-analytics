# People Analytics SQL Practice

These prompts use Atlas model names and Snowflake-style SQL. They are designed
for interview preparation around SCD2, date spines, window functions,
deduplication, privacy-safe aggregation, and identity resolution.

## 1. Point-In-Time Headcount

**Prompt:** Return active headcount by department on `2026-01-31`.

```sql
select
    department,
    count(distinct canonical_person_id) as active_headcount
from {{ ref('fct_workforce_daily') }}
where snapshot_date = '2026-01-31'
  and is_active_on_date
group by 1
order by 2 desc;
```

What this tests: point-in-time logic and distinct canonical people.

## 2. Monthly Attrition Rate

**Prompt:** Calculate monthly attrition by department using month-start
headcount as the denominator.

```sql
with month_start as (
    select
        date_trunc('month', snapshot_date) as month_start_date,
        department,
        count(distinct canonical_person_id) as start_headcount
    from {{ ref('fct_workforce_daily') }}
    where snapshot_date = date_trunc('month', snapshot_date)
      and is_active_on_date
    group by 1, 2
),

terminations as (
    select
        date_trunc('month', snapshot_date) as month_start_date,
        department,
        count(distinct canonical_person_id) as terminations
    from {{ ref('fct_workforce_daily') }}
    where is_termination_date
    group by 1, 2
)

select
    ms.month_start_date,
    ms.department,
    ms.start_headcount,
    coalesce(t.terminations, 0) as terminations,
    coalesce(t.terminations, 0) / nullif(ms.start_headcount, 0) as attrition_rate
from month_start as ms
left join terminations as t
    on ms.month_start_date = t.month_start_date
   and ms.department = t.department
order by 1, 2;
```

What this tests: denominator choice and event/date alignment.

## 3. Current Employee Row From SCD2

**Prompt:** Return the current row per employee spell from `dim_employee`.

```sql
select *
from {{ ref('dim_employee') }}
qualify row_number() over (
    partition by employee_spell_key
    order by effective_from desc
) = 1;
```

What this tests: SCD2 version selection with `qualify`.

## 4. Detect Broken SCD2 Contiguity

**Prompt:** Find employee spell versions where `effective_to` overlaps the next
version.

```sql
with sequenced as (
    select
        employee_spell_key,
        effective_from,
        effective_to,
        lead(effective_from) over (
            partition by employee_spell_key
            order by effective_from
        ) as next_effective_from
    from {{ ref('dim_employee') }}
)

select *
from sequenced
where next_effective_from is not null
  and effective_to >= next_effective_from;
```

What this tests: window functions and SCD2 quality checks.

## 5. Rehire Detection

**Prompt:** Find canonical people with more than one HRIS employment spell.

```sql
select
    canonical_person_id,
    count(distinct hris_employee_id) as employment_spells,
    min(hire_date) as first_hire_date,
    max(hire_date) as latest_hire_date
from {{ ref('dim_employee') }}
group by 1
having count(distinct hris_employee_id) > 1
order by employment_spells desc, latest_hire_date desc;
```

What this tests: stable canonical identity across source-system IDs.

## 6. Stewardship Queue Coverage

**Prompt:** Summarize unresolved source identities by source system and reason.

```sql
select
    source_system,
    stewardship_reason,
    count(*) as unresolved_records
from {{ ref('int_stewardship_queue') }}
group by 1, 2
order by unresolved_records desc;
```

What this tests: treating unmatched records as a controlled workflow.

## 7. Privacy-Safe Headcount

**Prompt:** Return daily headcount while suppressing cohorts below k.

```sql
select
    snapshot_date,
    department,
    location,
    employment_type,
    case
        when count(distinct canonical_person_id) >= 5
            then count(distinct canonical_person_id)
    end as headcount,
    count(distinct canonical_person_id) >= 5 as is_reportable
from {{ ref('fct_workforce_daily') }}
where is_active_on_date
group by 1, 2, 3, 4
order by 1, 2, 3, 4;
```

What this tests: aggregate privacy controls and safe public surfaces.

## 8. Match Candidate Ranking

**Prompt:** Within each source record, keep the highest-scoring residual
candidate.

```sql
select *
from residual_candidates
qualify row_number() over (
    partition by source_record_id
    order by score desc, evidence_weight desc, candidate_canonical_person_id
) = 1;
```

What this tests: deterministic tie-breaking in review workflows.

## 9. HR And Finance Reconciliation

**Prompt:** Compare HRIS active headcount to a Finance control table by month.

```sql
with atlas_headcount as (
    select
        date_trunc('month', snapshot_date) as month_start_date,
        count(distinct canonical_person_id) as atlas_active_headcount
    from {{ ref('fct_workforce_daily') }}
    where snapshot_date = last_day(snapshot_date)
      and is_active_on_date
    group by 1
)

select
    a.month_start_date,
    a.atlas_active_headcount,
    f.finance_active_headcount,
    a.atlas_active_headcount - f.finance_active_headcount as variance
from atlas_headcount as a
join finance_headcount_control as f
    on a.month_start_date = f.month_start_date
where abs(a.atlas_active_headcount - f.finance_active_headcount) > 0
order by 1;
```

What this tests: reconciliation mindset and clear variance surfacing.

## 10. Duplicate Hard Anchor Detection

**Prompt:** Find cases where the same DOB and work-email local part map to more
than one canonical person.

```sql
select
    date_of_birth,
    work_email_local_part,
    count(distinct canonical_person_id) as canonical_people
from {{ ref('int_canonical_person') }}
where date_of_birth is not null
  and work_email_local_part is not null
group by 1, 2
having count(distinct canonical_person_id) > 1;
```

What this tests: identity invariant thinking.

## How To Narrate These In Interview

For each answer, name the business rule first, then the SQL construct:

- "Headcount is point-in-time, so I query the daily fact on the reporting date."
- "Attrition needs a denominator definition, so I use month-start population."
- "SCD2 needs version ordering, so I use window functions and `qualify`."
- "People data needs privacy controls, so I suppress small cohorts before the
  dashboard layer."
- "Identity matching needs tests around invariants, not just row counts."
