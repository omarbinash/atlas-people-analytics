{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'stewardship_queue']
    )
}}

-- =============================================================================
-- int_stewardship_queue — Phase 2C manual review surface
-- =============================================================================
-- One row per non-HRIS source identity that did not qualify for deterministic
-- auto-merge. This is not a failure table; it is the control surface that keeps
-- the matcher conservative. HR stewards can adjudicate these cases with the
-- best available candidate evidence without the model silently creating a bad
-- canonical employee record.
--
-- The queue intentionally avoids SIN_LAST_4 and other government identifiers.
-- Email is decomposed into local part + domain so reviewers have enough context
-- for synthetic demos without propagating full personal email addresses.
-- =============================================================================

with nodes as (
    select *
    from {{ ref('int_identity_source_nodes') }}
    where source_system != 'HRIS'
),

auto_matches as (
    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified

    union all

    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_2_name_dob_hire') }}
    where auto_merge_qualified

    union all

    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_3_email_domain') }}
    where auto_merge_qualified
),

all_candidates as (
    select
        source_record_key,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        candidate_hris_person_count,
        hire_date_diff_days,
        hris_first_name,
        hris_last_name,
        hris_hire_date
    from {{ ref('int_identity_pass_1_hard_anchors') }}

    union all

    select
        source_record_key,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        candidate_hris_person_count,
        hire_date_diff_days,
        hris_first_name,
        hris_last_name,
        hris_hire_date
    from {{ ref('int_identity_pass_2_name_dob_hire') }}

    union all

    select
        source_record_key,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        candidate_hris_person_count,
        hire_date_diff_days,
        hris_first_name,
        hris_last_name,
        hris_hire_date
    from {{ ref('int_identity_pass_3_email_domain') }}
),

best_candidate as (
    select *
    from all_candidates
    qualify row_number() over (
        partition by source_record_key
        order by match_score desc, match_pass asc, candidate_hris_person_count asc, hris_person_key asc
    ) = 1
),

unresolved as (
    select nodes.*
    from nodes
    left join auto_matches
        on nodes.source_record_key = auto_matches.source_record_key
    where auto_matches.source_record_key is null
)

select
    {{ dbt_utils.generate_surrogate_key(['unresolved.source_record_key']) }}  as stewardship_queue_id,
    unresolved.source_system,
    unresolved.source_record_key,
    unresolved.source_primary_id,
    unresolved.ats_candidate_id,
    unresolved.payroll_spell_key,
    unresolved.employee_payroll_id,
    unresolved.crm_user_id,
    unresolved.dms_erp_person_key,
    unresolved.dms_user_id,
    unresolved.erp_user_id,
    unresolved.merge_topology,

    unresolved.source_first_name,
    unresolved.source_last_name,
    unresolved.source_first_name_root,
    unresolved.source_last_name_norm,
    unresolved.source_hire_date,
    unresolved.source_end_date,
    unresolved.source_email_local_part,
    unresolved.source_email_domain,

    best_candidate.hris_person_key                                           as suggested_hris_person_key,
    case
        when best_candidate.hris_person_key is not null
            then 'cp_' || best_candidate.hris_person_key
    end                                                                      as suggested_canonical_person_id,
    best_candidate.hris_first_name                                           as suggested_hris_first_name,
    best_candidate.hris_last_name                                            as suggested_hris_last_name,
    best_candidate.hris_hire_date                                            as suggested_hris_hire_date,
    best_candidate.match_pass                                                as suggested_match_pass,
    best_candidate.match_rule                                                as suggested_match_rule,
    best_candidate.match_score                                               as suggested_match_score,
    best_candidate.match_anchor_count                                        as suggested_match_anchor_count,
    best_candidate.candidate_hris_person_count                               as candidate_hris_person_count,
    best_candidate.hire_date_diff_days                                       as suggested_hire_date_diff_days,

    case
        when best_candidate.source_record_key is null
            then 'NO_DETERMINISTIC_CANDIDATE'
        when best_candidate.candidate_hris_person_count > 1
            then 'AMBIGUOUS_CANDIDATES'
        when best_candidate.match_score < 0.95
            then 'BELOW_AUTO_MATCH_THRESHOLD'
        else 'MANUAL_REVIEW_REQUIRED'
    end                                                                      as stewardship_reason,

    unresolved.loaded_at,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from unresolved
left join best_candidate
    on unresolved.source_record_key = best_candidate.source_record_key
