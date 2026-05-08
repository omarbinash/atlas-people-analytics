{{
    config(
        materialized='table',
        tags=['marts', 'core', 'phase_2d', 'dim_employee']
    )
}}

-- =============================================================================
-- dim_employee - Phase 2D core SCD2-style employee dimension
-- =============================================================================
-- One row per canonical person employment spell. This is the effective-dated
-- employee dimension that downstream point-in-time facts join to.
--
-- Tradeoff: the current synthetic HRIS feed emits one row per employment spell,
-- with department/location/job title captured as the latest value inside that
-- spell. It does not emit a full intra-spell event log for every transfer. So
-- this dimension is SCD2 at the employment-spell grain, not at every internal
-- transfer grain. That is still the right Phase 2D step: it proves rehire and
-- termination point-in-time correctness without inventing source history that
-- the raw feed does not contain.
--
-- Privacy: full date_of_birth and SIN_LAST_4 are intentionally absent from this
-- mart. DOB remains in intermediate matching models where it is needed for
-- identity resolution; SIN stays in payroll staging/intermediate only.
-- =============================================================================

with canonical as (
    select * from {{ ref('int_canonical_person') }}
),

hris_persons as (
    select * from {{ ref('int_hris_persons') }}
),

hris_spells as (
    select * from {{ ref('stg_hris__employees') }}
),

joined as (
    select
        canonical.canonical_person_id,
        canonical.hris_person_key,
        hris.hris_employee_id,

        hris.hire_date                                                       as effective_start_date,
        case
            when hris.termination_date is not null
                then dateadd(day, -1, hris.termination_date)
            else to_date('9999-12-31')
        end                                                                  as effective_end_date,
        hris.termination_date                                                as termination_date,

        row_number() over (
            partition by canonical.canonical_person_id
            order by hris.hire_date desc, hris.hris_employee_id desc
        ) = 1                                                                as is_current_record,

        row_number() over (
            partition by canonical.canonical_person_id
            order by hris.hire_date asc, hris.hris_employee_id asc
        )                                                                    as employment_spell_number,

        hris.legal_first_name_original,
        hris.legal_last_name_original,
        hris.preferred_name_original,
        hris.legal_first_name,
        hris.legal_last_name,
        hris.preferred_name,

        hris.employment_status                                               as source_employment_status,
        hris.employment_type,
        hris.department,
        hris.job_title,
        hris.manager_hris_id,
        hris.location,

        canonical.has_rehires,
        canonical.has_name_change_marriage,
        canonical.ats_candidate_count,
        canonical.payroll_spell_count,
        canonical.crm_user_count,
        canonical.dms_user_count,
        canonical.erp_user_count,
        canonical.matched_external_source_system_count,
        canonical.match_passes_used,

        greatest(
            coalesce(hris.loaded_at, to_timestamp_ntz('1900-01-01')),
            coalesce(canonical.loaded_at, to_timestamp_ntz('1900-01-01'))
        )                                                                    as loaded_at
    from hris_spells hris
    inner join hris_persons persons
        on hris.date_of_birth = persons.date_of_birth
       and hris.personal_email_local_part = persons.personal_email_local_part
    inner join canonical
        on persons.hris_person_key = canonical.hris_person_key
)

select
    {{ dbt_utils.generate_surrogate_key([
        'canonical_person_id',
        'hris_employee_id',
        'effective_start_date'
    ]) }}                                                                    as employee_sk,
    joined.*,
    case
        when termination_date is null then true
        else false
    end                                                                      as is_open_ended_spell,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from joined
