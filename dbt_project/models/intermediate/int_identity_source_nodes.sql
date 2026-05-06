{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'identity_source_nodes']
    )
}}

-- =============================================================================
-- int_identity_source_nodes — Phase 2C common matching grain
-- =============================================================================
-- Puts each matchable source-system identity onto one normalized shape before
-- the deterministic passes run. This model deliberately keeps the grain close
-- to the operational systems:
--
--   HRIS      one row per HRIS-distinct person from int_hris_persons
--   ATS       one row per candidate/application
--   PAYROLL   one row per payroll spell from int_payroll_spells
--   CRM       one row per CRM user
--   DMS_ERP   one row per unified DMS/ERP topology component
--
-- DMS and ERP are represented together because int_dms_erp_unified has already
-- consumed the hard structural FK. The individual dms_user_id / erp_user_id
-- columns remain here so coverage tests can still prove every staging row is
-- either resolved or queued for stewardship.
--
-- Design note: this layer normalizes names and email anchors only. It does not
-- decide identity. False positives in HR data are worse than false negatives,
-- so deterministic decisions remain isolated in the three pass models where
-- their evidence and thresholds are auditable.
-- =============================================================================

with hris as (
    select * from {{ ref('int_hris_persons') }}
),

ats as (
    select * from {{ ref('stg_ats__candidates') }}
),

payroll as (
    select * from {{ ref('int_payroll_spells') }}
),

crm as (
    select * from {{ ref('stg_crm__sales_reps') }}
),

dms_erp as (
    select * from {{ ref('int_dms_erp_unified') }}
),

hris_nodes as (
    select
        'HRIS'                                                               as source_system,
        'HRIS_PERSON::' || h.hris_person_key                                 as source_record_key,
        h.hris_person_key                                                    as source_primary_id,
        h.hris_person_key                                                    as hris_person_key,

        h.canonical_legal_first_name                                         as source_first_name,
        h.canonical_legal_last_name                                          as source_last_name,
        h.canonical_legal_first_name_original                                as source_first_name_original,
        h.canonical_legal_last_name_original                                 as source_last_name_original,
        {{ first_name_root('h.canonical_legal_first_name', 'hris_nm') }}      as source_first_name_root,
        {{ normalize_name('h.canonical_legal_last_name') }}                  as source_last_name_norm,

        h.date_of_birth                                                      as date_of_birth,
        h.canonical_hire_date                                                as source_hire_date,
        h.latest_termination_date                                            as source_end_date,

        h.personal_email_local_part                                          as personal_email_local_part,
        case
            when h.personal_email is not null and position('@' in h.personal_email) > 0
                then split_part(h.personal_email, '@', 2)
        end                                                                  as personal_email_domain,
        h.work_email_local_part                                              as work_email_local_part,
        case
            when h.work_email is not null and position('@' in h.work_email) > 0
                then split_part(h.work_email, '@', 2)
        end                                                                  as work_email_domain,

        h.work_email_local_part                                              as source_email_local_part,
        case
            when h.work_email is not null and position('@' in h.work_email) > 0
                then split_part(h.work_email, '@', 2)
        end                                                                  as source_email_domain,

        cast(null as varchar)                                                as ats_candidate_id,
        cast(null as varchar)                                                as payroll_spell_key,
        cast(null as varchar)                                                as employee_payroll_id,
        cast(null as varchar)                                                as crm_user_id,
        cast(null as varchar)                                                as dms_erp_person_key,
        cast(null as varchar)                                                as dms_user_id,
        cast(null as varchar)                                                as erp_user_id,
        cast(null as varchar)                                                as merge_topology,
        cast(null as boolean)                                                as has_dms,
        cast(null as boolean)                                                as has_erp,
        cast(null as boolean)                                                as has_broken_link,

        h.loaded_at                                                          as loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from hris h
    left join {{ ref('nickname_map') }} hris_nm
        on hris_nm.nickname = {{ normalize_name('h.canonical_legal_first_name') }}
),

ats_nodes as (
    select
        'ATS'                                                                as source_system,
        'ATS::' || a.ats_candidate_id                                        as source_record_key,
        a.ats_candidate_id                                                   as source_primary_id,
        cast(null as varchar)                                                as hris_person_key,

        a.preferred_first_name                                               as source_first_name,
        a.last_name                                                          as source_last_name,
        a.preferred_first_name_original                                      as source_first_name_original,
        a.last_name_original                                                 as source_last_name_original,
        {{ first_name_root('a.preferred_first_name', 'ats_nm') }}             as source_first_name_root,
        {{ normalize_name('a.last_name') }}                                  as source_last_name_norm,

        cast(null as date)                                                   as date_of_birth,
        a.offer_accepted_date                                                as source_hire_date,
        cast(null as date)                                                   as source_end_date,

        a.email_local_part                                                   as personal_email_local_part,
        case
            when a.email is not null and position('@' in a.email) > 0
                then split_part(a.email, '@', 2)
        end                                                                  as personal_email_domain,
        cast(null as varchar)                                                as work_email_local_part,
        cast(null as varchar)                                                as work_email_domain,

        a.email_local_part                                                   as source_email_local_part,
        case
            when a.email is not null and position('@' in a.email) > 0
                then split_part(a.email, '@', 2)
        end                                                                  as source_email_domain,

        a.ats_candidate_id                                                   as ats_candidate_id,
        cast(null as varchar)                                                as payroll_spell_key,
        cast(null as varchar)                                                as employee_payroll_id,
        cast(null as varchar)                                                as crm_user_id,
        cast(null as varchar)                                                as dms_erp_person_key,
        cast(null as varchar)                                                as dms_user_id,
        cast(null as varchar)                                                as erp_user_id,
        cast(null as varchar)                                                as merge_topology,
        cast(null as boolean)                                                as has_dms,
        cast(null as boolean)                                                as has_erp,
        cast(null as boolean)                                                as has_broken_link,

        a.loaded_at                                                          as loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from ats a
    left join {{ ref('nickname_map') }} ats_nm
        on ats_nm.nickname = {{ normalize_name('a.preferred_first_name') }}
),

payroll_nodes as (
    select
        'PAYROLL'                                                            as source_system,
        'PAYROLL::' || p.payroll_spell_key                                   as source_record_key,
        p.employee_payroll_id                                                as source_primary_id,
        cast(null as varchar)                                                as hris_person_key,

        p.first_observed_legal_first_name                                    as source_first_name,
        p.first_observed_legal_last_name                                     as source_last_name,
        p.first_observed_legal_first_name_original                           as source_first_name_original,
        p.first_observed_legal_last_name_original                            as source_last_name_original,
        {{ first_name_root('p.first_observed_legal_first_name', 'pay_nm') }}  as source_first_name_root,
        {{ normalize_name('p.first_observed_legal_last_name') }}             as source_last_name_norm,

        cast(null as date)                                                   as date_of_birth,
        p.first_pay_period_start                                             as source_hire_date,
        p.most_recent_pay_period_end                                         as source_end_date,

        cast(null as varchar)                                                as personal_email_local_part,
        cast(null as varchar)                                                as personal_email_domain,
        cast(null as varchar)                                                as work_email_local_part,
        cast(null as varchar)                                                as work_email_domain,
        cast(null as varchar)                                                as source_email_local_part,
        cast(null as varchar)                                                as source_email_domain,

        cast(null as varchar)                                                as ats_candidate_id,
        p.payroll_spell_key                                                  as payroll_spell_key,
        p.employee_payroll_id                                                as employee_payroll_id,
        cast(null as varchar)                                                as crm_user_id,
        cast(null as varchar)                                                as dms_erp_person_key,
        cast(null as varchar)                                                as dms_user_id,
        cast(null as varchar)                                                as erp_user_id,
        cast(null as varchar)                                                as merge_topology,
        cast(null as boolean)                                                as has_dms,
        cast(null as boolean)                                                as has_erp,
        cast(null as boolean)                                                as has_broken_link,

        p.loaded_at                                                          as loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from payroll p
    left join {{ ref('nickname_map') }} pay_nm
        on pay_nm.nickname = {{ normalize_name('p.first_observed_legal_first_name') }}
),

crm_nodes as (
    select
        'CRM'                                                                as source_system,
        'CRM::' || c.crm_user_id                                             as source_record_key,
        c.crm_user_id                                                        as source_primary_id,
        cast(null as varchar)                                                as hris_person_key,

        c.preferred_first_name                                               as source_first_name,
        c.last_name                                                          as source_last_name,
        c.preferred_first_name_original                                      as source_first_name_original,
        c.last_name_original                                                 as source_last_name_original,
        {{ first_name_root('c.preferred_first_name', 'crm_nm') }}             as source_first_name_root,
        {{ normalize_name('c.last_name') }}                                  as source_last_name_norm,

        cast(null as date)                                                   as date_of_birth,
        cast(c.created_at as date)                                           as source_hire_date,
        cast(c.deactivated_at as date)                                       as source_end_date,

        cast(null as varchar)                                                as personal_email_local_part,
        cast(null as varchar)                                                as personal_email_domain,
        c.crm_email_local_part                                               as work_email_local_part,
        case
            when c.crm_email is not null and position('@' in c.crm_email) > 0
                then split_part(c.crm_email, '@', 2)
        end                                                                  as work_email_domain,
        c.crm_email_local_part                                               as source_email_local_part,
        case
            when c.crm_email is not null and position('@' in c.crm_email) > 0
                then split_part(c.crm_email, '@', 2)
        end                                                                  as source_email_domain,

        cast(null as varchar)                                                as ats_candidate_id,
        cast(null as varchar)                                                as payroll_spell_key,
        cast(null as varchar)                                                as employee_payroll_id,
        c.crm_user_id                                                        as crm_user_id,
        cast(null as varchar)                                                as dms_erp_person_key,
        cast(null as varchar)                                                as dms_user_id,
        cast(null as varchar)                                                as erp_user_id,
        cast(null as varchar)                                                as merge_topology,
        cast(null as boolean)                                                as has_dms,
        cast(null as boolean)                                                as has_erp,
        cast(null as boolean)                                                as has_broken_link,

        c.loaded_at                                                          as loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from crm c
    left join {{ ref('nickname_map') }} crm_nm
        on crm_nm.nickname = {{ normalize_name('c.preferred_first_name') }}
),

dms_erp_nodes as (
    select
        'DMS_ERP'                                                            as source_system,
        'DMS_ERP::' || d.dms_erp_person_key                                  as source_record_key,
        d.dms_erp_person_key                                                 as source_primary_id,
        cast(null as varchar)                                                as hris_person_key,

        d.short_first_name                                                   as source_first_name,
        d.last_name                                                          as source_last_name,
        d.short_first_name_original                                          as source_first_name_original,
        d.last_name_original                                                 as source_last_name_original,
        {{ first_name_root('d.short_first_name', 'dms_nm') }}                 as source_first_name_root,
        {{ normalize_name('d.last_name') }}                                  as source_last_name_norm,

        cast(null as date)                                                   as date_of_birth,
        coalesce(d.hire_date_dms, cast(d.erp_created_at as date))             as source_hire_date,
        d.terminated_date_dms                                                as source_end_date,

        cast(null as varchar)                                                as personal_email_local_part,
        cast(null as varchar)                                                as personal_email_domain,
        d.erp_email_local_part                                               as work_email_local_part,
        case
            when d.erp_email is not null and position('@' in d.erp_email) > 0
                then split_part(d.erp_email, '@', 2)
        end                                                                  as work_email_domain,
        d.erp_email_local_part                                               as source_email_local_part,
        case
            when d.erp_email is not null and position('@' in d.erp_email) > 0
                then split_part(d.erp_email, '@', 2)
        end                                                                  as source_email_domain,

        cast(null as varchar)                                                as ats_candidate_id,
        cast(null as varchar)                                                as payroll_spell_key,
        cast(null as varchar)                                                as employee_payroll_id,
        cast(null as varchar)                                                as crm_user_id,
        d.dms_erp_person_key                                                 as dms_erp_person_key,
        d.dms_user_id                                                        as dms_user_id,
        d.erp_user_id                                                        as erp_user_id,
        d.merge_topology                                                     as merge_topology,
        d.has_dms                                                            as has_dms,
        d.has_erp                                                            as has_erp,
        d.has_broken_link                                                    as has_broken_link,

        d.loaded_at                                                          as loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from dms_erp d
    left join {{ ref('nickname_map') }} dms_nm
        on dms_nm.nickname = {{ normalize_name('d.short_first_name') }}
)

select * from hris_nodes
union all
select * from ats_nodes
union all
select * from payroll_nodes
union all
select * from crm_nodes
union all
select * from dms_erp_nodes
