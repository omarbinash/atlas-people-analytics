{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'canonical_person']
    )
}}

-- =============================================================================
-- int_canonical_person — Phase 2C unified identity output
-- =============================================================================
-- One row per canonical person, seeded from int_hris_persons and enriched with
-- source-system identifiers that passed deterministic auto-merge.
--
-- The canonical_person_id is a stable, non-PII surrogate derived from the HRIS
-- person key. That key already collapses rehires and contractor-to-FTE HRIS ID
-- churn by grouping on DOB + personal-email local part, so the emitted ID
-- survives the lifecycle drift this project is designed to demonstrate.
--
-- Sensitive fields such as SIN_LAST_4 are intentionally absent. Payroll is
-- represented only by its spell identifiers and aggregate spell context; marts
-- must not expose payroll government identifiers.
-- =============================================================================

with hris_persons as (
    select * from {{ ref('int_hris_persons') }}
),

auto_matches as (
    select
        source_system,
        source_record_key,
        source_primary_id,
        ats_candidate_id,
        payroll_spell_key,
        employee_payroll_id,
        crm_user_id,
        dms_erp_person_key,
        dms_user_id,
        erp_user_id,
        merge_topology,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        loaded_at
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified

    union all

    select
        source_system,
        source_record_key,
        source_primary_id,
        ats_candidate_id,
        payroll_spell_key,
        employee_payroll_id,
        crm_user_id,
        dms_erp_person_key,
        dms_user_id,
        erp_user_id,
        merge_topology,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        loaded_at
    from {{ ref('int_identity_pass_2_name_dob_hire') }}
    where auto_merge_qualified

    union all

    select
        source_system,
        source_record_key,
        source_primary_id,
        ats_candidate_id,
        payroll_spell_key,
        employee_payroll_id,
        crm_user_id,
        dms_erp_person_key,
        dms_user_id,
        erp_user_id,
        merge_topology,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        loaded_at
    from {{ ref('int_identity_pass_3_email_domain') }}
    where auto_merge_qualified
),

best_matches as (
    select *
    from auto_matches
    qualify row_number() over (
        partition by source_record_key
        order by match_pass asc, match_score desc, hris_person_key asc
    ) = 1
),

match_summary as (
    select
        hris_person_key,
        count(*)                                                             as matched_external_source_node_count,
        count(distinct source_system)                                        as matched_external_source_system_count,
        listagg(distinct to_varchar(match_pass), ',')
            within group (order by to_varchar(match_pass))                   as match_passes_used,
        max(loaded_at)                                                       as external_loaded_at
    from best_matches
    group by hris_person_key
),

ats_matches as (
    select
        hris_person_key,
        array_agg(distinct ats_candidate_id) within group (order by ats_candidate_id)
                                                                               as ats_candidate_ids,
        count(distinct ats_candidate_id)                                      as ats_candidate_count
    from best_matches
    where source_system = 'ATS'
      and ats_candidate_id is not null
    group by hris_person_key
),

payroll_matches as (
    select
        hris_person_key,
        array_agg(distinct employee_payroll_id) within group (order by employee_payroll_id)
                                                                               as employee_payroll_ids,
        array_agg(distinct payroll_spell_key) within group (order by payroll_spell_key)
                                                                               as payroll_spell_keys,
        count(distinct employee_payroll_id)                                   as payroll_spell_count
    from best_matches
    where source_system = 'PAYROLL'
      and employee_payroll_id is not null
    group by hris_person_key
),

crm_matches as (
    select
        hris_person_key,
        array_agg(distinct crm_user_id) within group (order by crm_user_id)   as crm_user_ids,
        count(distinct crm_user_id)                                           as crm_user_count
    from best_matches
    where source_system = 'CRM'
      and crm_user_id is not null
    group by hris_person_key
),

dms_matches as (
    select
        hris_person_key,
        array_agg(distinct dms_user_id) within group (order by dms_user_id)   as dms_user_ids,
        count(distinct dms_user_id)                                           as dms_user_count
    from best_matches
    where source_system = 'DMS_ERP'
      and dms_user_id is not null
    group by hris_person_key
),

erp_matches as (
    select
        hris_person_key,
        array_agg(distinct erp_user_id) within group (order by erp_user_id)   as erp_user_ids,
        count(distinct erp_user_id)                                           as erp_user_count
    from best_matches
    where source_system = 'DMS_ERP'
      and erp_user_id is not null
    group by hris_person_key
)

select
    'cp_' || h.hris_person_key                                               as canonical_person_id,
    h.hris_person_key,

    -- HRIS anchor attributes
    h.hris_employee_ids,
    h.current_hris_employee_id,
    h.date_of_birth,
    h.personal_email_local_part,
    h.work_email_local_part,
    h.canonical_hire_date,
    h.latest_hire_date,
    h.latest_termination_date,
    h.canonical_legal_first_name,
    h.canonical_legal_last_name,
    h.canonical_legal_first_name_original,
    h.canonical_legal_last_name_original,
    h.current_legal_first_name,
    h.current_legal_last_name,
    h.current_preferred_name,
    h.current_employment_status,
    h.current_employment_type,
    h.current_department,
    h.current_job_title,
    h.current_location,
    h.current_manager_hris_id,
    h.spell_count                                                            as hris_spell_count,
    h.has_rehires,
    h.has_name_change_marriage,

    -- Resolved source identifiers. Empty arrays are easier for downstream
    -- consumers than NULL when a person does not appear in a system.
    coalesce(ats.ats_candidate_ids, array_construct())                       as ats_candidate_ids,
    coalesce(ats.ats_candidate_count, 0)                                     as ats_candidate_count,
    coalesce(payroll.employee_payroll_ids, array_construct())                as employee_payroll_ids,
    coalesce(payroll.payroll_spell_keys, array_construct())                  as payroll_spell_keys,
    coalesce(payroll.payroll_spell_count, 0)                                 as payroll_spell_count,
    coalesce(crm.crm_user_ids, array_construct())                            as crm_user_ids,
    coalesce(crm.crm_user_count, 0)                                          as crm_user_count,
    coalesce(dms.dms_user_ids, array_construct())                            as dms_user_ids,
    coalesce(dms.dms_user_count, 0)                                          as dms_user_count,
    coalesce(erp.erp_user_ids, array_construct())                            as erp_user_ids,
    coalesce(erp.erp_user_count, 0)                                          as erp_user_count,

    coalesce(summary.matched_external_source_node_count, 0)                  as matched_external_source_node_count,
    coalesce(summary.matched_external_source_system_count, 0)                 as matched_external_source_system_count,
    coalesce(summary.match_passes_used, '')                                  as match_passes_used,

    greatest(
        coalesce(h.loaded_at, to_timestamp_ntz('1900-01-01')),
        coalesce(summary.external_loaded_at, to_timestamp_ntz('1900-01-01'))
    )                                                                        as loaded_at,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from hris_persons h
left join match_summary summary
    on h.hris_person_key = summary.hris_person_key
left join ats_matches ats
    on h.hris_person_key = ats.hris_person_key
left join payroll_matches payroll
    on h.hris_person_key = payroll.hris_person_key
left join crm_matches crm
    on h.hris_person_key = crm.hris_person_key
left join dms_matches dms
    on h.hris_person_key = dms.hris_person_key
left join erp_matches erp
    on h.hris_person_key = erp.hris_person_key
