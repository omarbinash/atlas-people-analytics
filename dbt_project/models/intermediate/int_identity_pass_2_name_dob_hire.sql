{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'identity_pass_2']
    )
}}

-- =============================================================================
-- int_identity_pass_2_name_dob_hire - normalized name + DOB + hire proximity
-- =============================================================================
-- Pass 2 implements the locked deterministic rule:
--
--   normalized first-name-root + normalized last-name
--   AND date_of_birth exact
--   AND hire date within +/- 30 days of an HRIS employment spell
--
-- Most non-HRIS sources in the current synthetic schema do not expose DOB, so
-- this model often produces candidate evidence without auto-merging it. That is
-- intentional. A name + hire-date match can be useful to HR stewardship, but it
-- is not enough to silently merge people in People Analytics data.
-- =============================================================================

with nodes as (
    select * from {{ ref('int_identity_source_nodes') }}
),

pass_1_auto as (
    select distinct source_record_key
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified
),

sources as (
    select nodes.*
    from nodes
    left join pass_1_auto
        on nodes.source_record_key = pass_1_auto.source_record_key
    where nodes.source_system != 'HRIS'
      and pass_1_auto.source_record_key is null
      and nodes.source_first_name_root is not null
      and nodes.source_last_name_norm is not null
      and nodes.source_hire_date is not null
),

hris_spells as (
    select
        persons.hris_person_key,
        hris.hris_employee_id,
        hris.date_of_birth,
        hris.hire_date,
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
        2                                                                    as match_pass,
        'normalized_name_dob_hire_proximity'                                 as match_rule,
        abs(datediff(day, src.source_hire_date, hris.hire_date))              as hire_date_diff_days,

        src.date_of_birth is not null                                         as source_has_dob,
        src.date_of_birth = hris.date_of_birth                                as dob_match,
        true                                                                 as name_match,

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
        on src.source_last_name_norm = hris.hris_last_name_norm
       and src.source_first_name_root in (
            hris.hris_legal_first_name_root,
            hris.hris_preferred_first_name_root
       )
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
            when source_has_dob and dob_match and hire_date_diff_days = 0 then 0.98
            when source_has_dob and dob_match and hire_date_diff_days <= 7 then 0.97
            when source_has_dob and dob_match and hire_date_diff_days <= 30 then 0.96
            when hire_date_diff_days = 0 then 0.70
            when hire_date_diff_days <= 7 then 0.65
            else 0.60
        end::float as match_score,
        case
            when source_has_dob and dob_match then 3
            else 2
        end as match_anchor_count
    from candidates
    inner join candidate_counts
        on candidates.source_record_key = candidate_counts.source_record_key
)

select
    scored.*,
    case
        when candidate_hris_person_count = 1
         and source_has_dob
         and dob_match
         and match_score >= 0.95
            then true
        else false
    end as auto_merge_qualified
from scored
