{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'pass_1_prep']
    )
}}

-- =============================================================================
-- int_hris_persons - Phase 2C, Step 1 prep
-- =============================================================================
-- Collapses HRIS rehires + contractor-to-FTE transitions into one row per
-- HRIS-distinct person. Each input row in stg_hris__employees represents one
-- employment spell (hire-to-termination); a single canonical person can have
-- multiple spells across rehires, each with a different HRIS_EMPLOYEE_ID
-- (suffixes _R1, _R2, _FTE per the synthesizer's _id_for_system).
--
-- ---------------------------------------------------------------------------
-- Grouping key: (date_of_birth, personal_email_local_part)
-- ---------------------------------------------------------------------------
-- Why these two columns:
--   * date_of_birth is immutable identity attribute, set at hire and never
--     updated. Same person across rehires has the same DOB.
--   * personal_email_local_part is set at canonical-identity creation
--     (synthesize.py: identity.personal_email) and persists across every
--     employment spell - marriage doesn't change it, rehire doesn't change it.
--
-- Why not (date_of_birth, normalize_name(legal_last_name))? Because
-- legal_last_name CAN change between spells: a marriage event during spell N
-- mutates the in-memory current_last, and the row emitted at TERMINATION of
-- spell N captures the post-marriage name. The next rehire (spell N+1)
-- starts with that post-marriage name. So two spells for one person CAN
-- have different last_names if any prior spell saw a marriage event. Last-
-- name grouping would silently fail to collapse those cases - we'd see them
-- as two distinct persons.
--
-- Why not date_of_birth alone? In a 5K-employee population with ~14,600
-- distinct DOBs in working-age range, expected DOB collisions are ~900+
-- (~18% of population shares DOB with at least one other person). DOB
-- alone is insufficient to distinguish people.
--
-- ---------------------------------------------------------------------------
-- Marriage name handling
-- ---------------------------------------------------------------------------
-- Per the locked design: the "canonical" identity tuple uses MIN(hire_date)
-- to anchor on the earliest observed name (pre any marriage drift). The
-- "current" snapshot uses MAX(hire_date) to reflect the latest known state.
-- Both are exposed for downstream use:
--   canonical_*  -> stable, anchors the cross-source canonical_person_id
--   current_*    -> latest, used for display and operational reporting
--
-- The has_name_change_marriage flag fires when canonical_legal_last_name
-- differs from current_legal_last_name. Useful for downstream auditing.
--
-- ---------------------------------------------------------------------------
-- Output grain: one row per HRIS-distinct person. Output `hris_person_key`
-- is the surrogate hash of the grouping key - NOT the final cross-source
-- canonical_person_id. That gets computed in int_canonical_person after all
-- passes have run.
-- =============================================================================

with hris as (
    select * from {{ ref('stg_hris__employees') }}
),

ranked as (
    -- Rank spells within each (DOB, personal_email_local_part) group:
    --   spell_rank_asc  = 1 -> earliest hire_date (canonical)
    --   spell_rank_desc = 1 -> latest hire_date   (current state)
    select
        h.*,
        row_number() over (
            partition by date_of_birth, personal_email_local_part
            order by hire_date asc, hris_employee_id asc
        ) as spell_rank_asc,
        row_number() over (
            partition by date_of_birth, personal_email_local_part
            order by hire_date desc, hris_employee_id desc
        ) as spell_rank_desc
    from hris h
    where date_of_birth is not null
      and personal_email_local_part is not null
),

person_aggs as (
    select
        -- ---- Grouping key ----
        date_of_birth,
        personal_email_local_part,

        -- ---- Canonical (earliest-spell) anchors ----
        max(case when spell_rank_asc = 1 then hire_date end)                     as canonical_hire_date,
        max(case when spell_rank_asc = 1 then legal_first_name end)              as canonical_legal_first_name,
        max(case when spell_rank_asc = 1 then legal_last_name end)               as canonical_legal_last_name,
        max(case when spell_rank_asc = 1 then legal_first_name_original end)     as canonical_legal_first_name_original,
        max(case when spell_rank_asc = 1 then legal_last_name_original end)      as canonical_legal_last_name_original,
        max(case when spell_rank_asc = 1 then preferred_name end)                as canonical_preferred_name,

        -- ---- Current (latest-spell) snapshot ----
        max(case when spell_rank_desc = 1 then hris_employee_id end)             as current_hris_employee_id,
        max(case when spell_rank_desc = 1 then hire_date end)                    as latest_hire_date,
        max(case when spell_rank_desc = 1 then termination_date end)             as latest_termination_date,
        max(case when spell_rank_desc = 1 then legal_first_name end)             as current_legal_first_name,
        max(case when spell_rank_desc = 1 then legal_last_name end)              as current_legal_last_name,
        max(case when spell_rank_desc = 1 then preferred_name end)               as current_preferred_name,
        max(case when spell_rank_desc = 1 then employment_status end)            as current_employment_status,
        max(case when spell_rank_desc = 1 then employment_type end)              as current_employment_type,
        max(case when spell_rank_desc = 1 then department end)                   as current_department,
        max(case when spell_rank_desc = 1 then job_title end)                    as current_job_title,
        max(case when spell_rank_desc = 1 then location end)                     as current_location,
        max(case when spell_rank_desc = 1 then manager_hris_id end)              as current_manager_hris_id,

        -- ---- Constants across spells ----
        -- These values are guaranteed by the synthesizer to be stable across
        -- all spells for a given canonical person (set once at canonical
        -- identity creation). Using max() here is just to satisfy GROUP BY
        -- semantics; assert_constant_across_spells_* tests in the YAML enforce
        -- the invariant.
        max(personal_email)                                                       as personal_email,
        max(work_email)                                                           as work_email,
        max(work_email_local_part)                                                as work_email_local_part,

        -- ---- Spell aggregates ----
        count(*)                                                                  as spell_count,
        array_agg(distinct hris_employee_id) within group (order by hris_employee_id) as hris_employee_ids,
        max(loaded_at)                                                            as loaded_at
    from ranked
    group by date_of_birth, personal_email_local_part
),

with_flags_and_key as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'date_of_birth',
            'personal_email_local_part'
        ]) }}                                                                     as hris_person_key,
        person_aggs.*,
        case when spell_count > 1 then true else false end                        as has_rehires,
        case
            when canonical_legal_last_name != current_legal_last_name then true
            else false
        end                                                                       as has_name_change_marriage,
        '{{ invocation_id }}'                                                     as _dbt_invocation_id
    from person_aggs
)

select * from with_flags_and_key
