{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'identity_pass_3']
    )
}}

-- =============================================================================
-- int_identity_pass_3_email_domain - company domain + email last-name token
-- =============================================================================
-- Pass 3 catches cases where exact email-local-part matching fails because the
-- first-name component differs across systems (Robert/Bob, preferred names,
-- transliteration), but the company email domain and last-name token still line
-- up with HRIS.
--
-- Auto-merge is intentionally narrow:
--   * source and HRIS emails are on the same company domain
--   * the last token of the email local part matches
--   * the source lifecycle date is within +/- 30 days of an HRIS spell
--   * exactly one HRIS person is a candidate
--
-- The uniqueness gate is the guardrail. Common last names hired near the same
-- date route to stewardship instead of being guessed.
-- =============================================================================

with nodes as (
    select * from {{ ref('int_identity_source_nodes') }}
),

pass_1_auto as (
    select distinct source_record_key
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified
),

pass_2_auto as (
    select distinct source_record_key
    from {{ ref('int_identity_pass_2_name_dob_hire') }}
    where auto_merge_qualified
),

sources as (
    select
        nodes.*,
        regexp_substr(nodes.source_email_local_part, '[^.]+$')               as source_email_last_token
    from nodes
    left join pass_1_auto
        on nodes.source_record_key = pass_1_auto.source_record_key
    left join pass_2_auto
        on nodes.source_record_key = pass_2_auto.source_record_key
    where nodes.source_system in ('CRM', 'DMS_ERP')
      and pass_1_auto.source_record_key is null
      and pass_2_auto.source_record_key is null
      and nodes.source_email_local_part is not null
      and nodes.source_email_domain is not null
      and nodes.source_hire_date is not null
),

hris_spells as (
    select
        persons.hris_person_key,
        hris.hris_employee_id,
        hris.hire_date,
        hris.work_email_local_part,
        case
            when hris.work_email is not null and position('@' in hris.work_email) > 0
                then split_part(hris.work_email, '@', 2)
        end                                                                  as work_email_domain,
        regexp_substr(hris.work_email_local_part, '[^.]+$')                  as hris_email_last_token,
        {{ first_name_root('hris.legal_first_name', 'hris_legal_nm') }}       as hris_legal_first_name_root,
        {{ first_name_root('coalesce(hris.preferred_name, hris.legal_first_name)', 'hris_pref_nm') }}
                                                                               as hris_preferred_first_name_root,
        {{ normalize_name('hris.legal_last_name') }}                         as hris_last_name_norm,
        hris.legal_first_name                                                as hris_first_name,
        hris.legal_last_name                                                 as hris_last_name
    from {{ ref('stg_hris__employees') }} hris
    inner join {{ ref('int_hris_persons') }} persons
        on hris.date_of_birth = persons.date_of_birth
       and hris.personal_email_local_part = persons.personal_email_local_part
    left join {{ ref('nickname_map') }} hris_legal_nm
        on hris_legal_nm.nickname = {{ normalize_name('hris.legal_first_name') }}
    left join {{ ref('nickname_map') }} hris_pref_nm
        on hris_pref_nm.nickname = {{ normalize_name('coalesce(hris.preferred_name, hris.legal_first_name)') }}
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
        3                                                                    as match_pass,
        'company_email_domain_last_token_hire_unique'                        as match_rule,
        abs(datediff(day, src.source_hire_date, hris.hire_date))              as hire_date_diff_days,

        src.source_first_name,
        src.source_last_name,
        src.source_first_name_root,
        src.source_last_name_norm,
        src.source_hire_date,
        src.source_email_local_part,
        src.source_email_domain,

        hris.hris_first_name,
        hris.hris_last_name,
        hris.hris_legal_first_name_root                                      as hris_first_name_root,
        hris.hris_last_name_norm,
        hris.hire_date                                                       as hris_hire_date,

        src.loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from sources src
    inner join hris_spells hris
        on src.source_email_domain = hris.work_email_domain
       and src.source_email_last_token = hris.hris_email_last_token
       and abs(datediff(day, src.source_hire_date, hris.hire_date)) <= 30
),

candidate_counts as (
    select
        source_record_key,
        count(distinct hris_person_key) as candidate_hris_person_count
    from candidates
    group by source_record_key
),

scored as (
    select
        candidates.*,
        candidate_counts.candidate_hris_person_count,
        case
            when hire_date_diff_days = 0 then 0.97
            when hire_date_diff_days <= 7 then 0.96
            else 0.95
        end::float as match_score,
        2 as match_anchor_count
    from candidates
    inner join candidate_counts
        on candidates.source_record_key = candidate_counts.source_record_key
)

select
    scored.*,
    case
        when candidate_hris_person_count = 1
         and match_score >= 0.95
            then true
        else false
    end as auto_merge_qualified
from scored
