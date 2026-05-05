{{
    config(
        materialized='view',
        tags=['staging', 'ats']
    )
}}

-- =============================================================================
-- stg_ats__candidates
-- =============================================================================
-- 1:1 mirror of RAW_ATS_CANDIDATES.
-- ATS uses preferred name and personal email (no work email at this stage).
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_ATS_CANDIDATES') }}
),

renamed as (
    select
        trim(ats_candidate_id)                                        as ats_candidate_id,

        -- Names
        trim(preferred_first_name)                                    as preferred_first_name_original,
        trim(last_name)                                               as last_name_original,
        lower(trim(preferred_first_name))                             as preferred_first_name,
        lower(trim(last_name))                                        as last_name,

        -- Email
        lower(trim(email))                                            as email,
        case
            when email is not null and position('@' in email) > 0
                then lower(trim(split_part(email, '@', 1)))
        end                                                           as email_local_part,

        -- Phone (kept as-is; standardization happens at intermediate layer if needed)
        trim(phone)                                                   as phone,

        -- Application lifecycle
        application_date                                              as application_date,
        offer_accepted_date                                           as offer_accepted_date,
        trim(sourced_from)                                            as sourced_from,

        -- Requisition context
        trim(requisition_department)                                  as requisition_department,
        trim(requisition_job_title)                                   as requisition_job_title,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
