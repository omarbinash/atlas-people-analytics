{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'identity_pass_1']
    )
}}

-- =============================================================================
-- int_identity_pass_1_hard_anchors - government/email anchors
-- =============================================================================
-- Pass 1 is reserved for high-certainty anchors:
--
--   * ATS personal email exact match to HRIS personal email
--   * CRM / ERP work-email local-part exact match to HRIS work-email local-part
--
-- The design also allows SIN_LAST_4 + DOB as a hard government-identifier
-- anchor. In this synthetic source shape, HRIS does not expose SIN and payroll
-- does not expose DOB; the synthesizer also regenerates SIN_LAST_4 per pay
-- period. Using SIN here would create false confidence, so payroll government
-- ID matching is intentionally not implemented for Phase 2C's current data.
--
-- Deterministic over probabilistic: even exact email anchors must be unique
-- against HRIS before auto-merge. Collisions route to stewardship rather than
-- guessing.
-- =============================================================================

with nodes as (
    select * from {{ ref('int_identity_source_nodes') }}
),

sources as (
    select *
    from nodes
    where source_system != 'HRIS'
),

hris as (
    select *
    from nodes
    where source_system = 'HRIS'
),

candidates as (
    select
        src.source_system,
        src.source_record_key,
        src.source_primary_id,
        src.ats_candidate_id,
        src.payroll_spell_key,
        src.employee_payroll_id,
        src.crm_user_id,
        src.dms_erp_person_key,
        src.dms_user_id,
        src.erp_user_id,
        src.merge_topology,

        hris.hris_person_key,
        1                                                                    as match_pass,
        case
            when src.source_system = 'ATS'
                then 'personal_email_exact'
            when src.source_system in ('CRM', 'DMS_ERP')
                then 'work_email_local_part_exact'
        end                                                                  as match_rule,
        1.00::float                                                          as match_score,
        2                                                                    as match_anchor_count,
        cast(null as integer)                                                as hire_date_diff_days,

        src.source_first_name,
        src.source_last_name,
        src.source_first_name_root,
        src.source_last_name_norm,
        src.source_hire_date,
        src.source_email_local_part,
        src.source_email_domain,

        hris.source_first_name                                               as hris_first_name,
        hris.source_last_name                                                as hris_last_name,
        hris.source_first_name_root                                          as hris_first_name_root,
        hris.source_last_name_norm                                           as hris_last_name_norm,
        hris.source_hire_date                                                as hris_hire_date,

        src.loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from sources src
    inner join hris
        on (
            src.source_system = 'ATS'
            and src.personal_email_local_part is not null
            and hris.personal_email_local_part is not null
            and src.personal_email_local_part = hris.personal_email_local_part
            and coalesce(src.personal_email_domain, '') = coalesce(hris.personal_email_domain, '')
        )
        or (
            src.source_system in ('CRM', 'DMS_ERP')
            and src.source_email_local_part is not null
            and hris.work_email_local_part is not null
            and src.source_email_local_part = hris.work_email_local_part
            and coalesce(src.source_email_domain, '') = coalesce(hris.work_email_domain, '')
        )
),

candidate_counts as (
    select
        source_record_key,
        count(distinct hris_person_key) as candidate_hris_person_count
    from candidates
    group by source_record_key
)

select
    candidates.*,
    candidate_counts.candidate_hris_person_count,
    case
        when candidate_counts.candidate_hris_person_count = 1 then true
        else false
    end as auto_merge_qualified
from candidates
inner join candidate_counts
    on candidates.source_record_key = candidate_counts.source_record_key
