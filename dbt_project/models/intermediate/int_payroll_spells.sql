{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'pass_1_prep']
    )
}}

-- =============================================================================
-- int_payroll_spells — Phase 2C, Step 1 prep
-- =============================================================================
-- Collapses ~153K monthly pay-period rows in stg_payroll__records into one
-- row per payroll spell (~5K rows). The grouping key is EMPLOYEE_PAYROLL_ID,
-- which the synthesizer guarantees is stable across all pay periods within
-- a single employment spell (synthesize.py:479 — same payroll_emp_id used
-- for every monthly record in a spell).
--
-- This collapse is essential before downstream matching. Joining 153K
-- payroll rows against 5K HRIS persons would produce noise (multiple
-- candidate matches per pay period). Spell-level grain reduces that to a
-- 5K-vs-5K join.
--
-- ---------------------------------------------------------------------------
-- Discipline note
-- ---------------------------------------------------------------------------
-- EMPLOYEE_PAYROLL_ID embeds the canonical person_id in the synthesizer
-- (PAY{YYYYMM}-{person_id[1:]} per synthesize.py:479). DO NOT parse the
-- numeric suffix to recover person_id — that is the synthesizer's "oracle
-- leak" and using it would short-circuit the entire matcher. We use the
-- column ONLY for grouping equality.
--
-- The legitimate signal extracted here is "spell continuity": every pay
-- period within a single spell shares the same EMPLOYEE_PAYROLL_ID. That
-- equality is the medium-strength anchor (+0.20) used in match_confidence,
-- valid only WITHIN payroll for spell collapse — NOT as a cross-source
-- bridge to HRIS.
--
-- See ~/.claude/.../memory/synthesizer_quirks.md for the full discussion of
-- the leak and which uses are legitimate.
--
-- ---------------------------------------------------------------------------
-- Why not collapse via SIN_LAST_4 + name?
-- ---------------------------------------------------------------------------
-- The synthesizer regenerates SIN_LAST_4 for every monthly pay period
-- (synthesize.py:497, `random.randint(1000, 9999)`). It is NOT stable
-- within a spell, NOT stable within a person. Was originally proposed in
-- CLAUDE.md as an anchor; dropped from the anchor table after verification.
-- See synthesizer_quirks memory.
--
-- ---------------------------------------------------------------------------
-- Earliest vs latest naming
-- ---------------------------------------------------------------------------
-- Payroll's legal_first_name and legal_last_name are captured per pay-period
-- row but typically don't change within a spell (the synthesizer doesn't
-- model intra-spell payroll-side name updates). However, payroll lags HRIS
-- on marriage updates in real life; the data exposes both:
--
--   first_observed_*   -> name as of earliest pay period (most stable anchor)
--   most_recent_*      -> name as of latest pay period (latest known state)
--
-- For Pass 2 matching, prefer first_observed_legal_last_name on the payroll
-- side joined against canonical_legal_last_name on the HRIS side — both
-- anchored to "as of earliest known observation."
--
-- ---------------------------------------------------------------------------
-- SIN_LAST_4 handling
-- ---------------------------------------------------------------------------
-- Surfaced as an array_agg of distinct values per spell, purely for
-- diagnostic visibility. Should never be used as a matching anchor. The
-- count of distinct SIN_LAST_4 values within a spell will typically be
-- close to spell_pay_period_count (one new random value per period).
-- =============================================================================

with payroll as (
    select * from {{ ref('stg_payroll__records') }}
),

ranked as (
    select
        p.*,
        row_number() over (
            partition by employee_payroll_id
            order by pay_period_start asc, payroll_record_id asc
        ) as period_rank_asc,
        row_number() over (
            partition by employee_payroll_id
            order by pay_period_start desc, payroll_record_id desc
        ) as period_rank_desc
    from payroll p
    where employee_payroll_id is not null
),

spell_aggs as (
    select
        -- ---- Grouping key ----
        employee_payroll_id,

        -- ---- First-observed (earliest pay period) snapshot ----
        max(case when period_rank_asc = 1 then payroll_record_id end)             as first_payroll_record_id,
        min(pay_period_start)                                                     as first_pay_period_start,
        max(case when period_rank_asc = 1 then legal_first_name end)              as first_observed_legal_first_name,
        max(case when period_rank_asc = 1 then legal_last_name end)               as first_observed_legal_last_name,
        max(case when period_rank_asc = 1 then legal_first_name_original end)     as first_observed_legal_first_name_original,
        max(case when period_rank_asc = 1 then legal_last_name_original end)      as first_observed_legal_last_name_original,

        -- ---- Most-recent (latest pay period) snapshot ----
        max(case when period_rank_desc = 1 then payroll_record_id end)            as most_recent_payroll_record_id,
        max(pay_period_end)                                                       as most_recent_pay_period_end,
        max(case when period_rank_desc = 1 then legal_first_name end)             as most_recent_legal_first_name,
        max(case when period_rank_desc = 1 then legal_last_name end)              as most_recent_legal_last_name,
        max(case when period_rank_desc = 1 then job_code end)                     as most_recent_job_code,
        max(case when period_rank_desc = 1 then cost_center end)                  as most_recent_cost_center,

        -- ---- Spell aggregates ----
        count(*)                                                                  as pay_period_count,
        sum(gross_amount_cad)                                                     as gross_amount_cad_total,
        sum(hours_worked)                                                         as hours_worked_total,
        avg(gross_amount_cad)                                                     as gross_amount_cad_avg_per_period,

        -- ---- Diagnostic surfaces ----
        -- SIN_LAST_4 is unstable within a spell (regenerated per pay period
        -- by the synthesizer). Surfaced as count of distinct values for
        -- visibility — should equal pay_period_count for any spell longer
        -- than a few periods. NEVER use as a matching anchor.
        count(distinct sin_last_4)                                                as sin_last_4_distinct_count,

        max(loaded_at)                                                            as loaded_at
    from ranked
    group by employee_payroll_id
),

with_flags_and_key as (
    select
        {{ dbt_utils.generate_surrogate_key(['employee_payroll_id']) }}           as payroll_spell_key,
        spell_aggs.*,
        case
            when most_recent_legal_last_name != first_observed_legal_last_name then true
            else false
        end                                                                       as has_intra_spell_last_name_change,
        '{{ invocation_id }}'                                                     as _dbt_invocation_id
    from spell_aggs
)

select * from with_flags_and_key
