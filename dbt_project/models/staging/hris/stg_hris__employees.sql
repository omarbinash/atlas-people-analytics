{{
    config(
        materialized='view',
        tags=['staging', 'hris']
    )
}}

-- =============================================================================
-- stg_hris__employees
-- =============================================================================
-- 1:1 mirror of RAW_HRIS_EMPLOYEES with:
--   - lowercase column names
--   - trimmed strings
--   - explicit nulls (empty strings -> NULL)
--   - email_local_part extracted (useful identity anchor)
--
-- No joins, no business logic, no de-duplication. This layer's job is to be
-- predictable and boring.
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_HRIS_EMPLOYEES') }}
),

renamed as (
    select
        -- Identifiers
        trim(hris_employee_id)                                       as hris_employee_id,

        -- Names: trim and lower-case for cross-system matching consistency.
        -- We KEEP the original casing in `*_original` columns so analysts can
        -- still display names properly downstream.
        trim(legal_first_name)                                        as legal_first_name_original,
        trim(legal_last_name)                                         as legal_last_name_original,
        nullif(trim(preferred_name), '')                              as preferred_name_original,

        lower(trim(legal_first_name))                                 as legal_first_name,
        lower(trim(legal_last_name))                                  as legal_last_name,
        lower(nullif(trim(preferred_name), ''))                       as preferred_name,

        -- Dates
        date_of_birth                                                 as date_of_birth,
        hire_date                                                     as hire_date,
        termination_date                                              as termination_date,

        -- Emails: lower and trim. Local part is everything before '@'.
        lower(trim(personal_email))                                   as personal_email,
        lower(trim(work_email))                                       as work_email,
        case
            when work_email is not null and position('@' in work_email) > 0
                then lower(trim(split_part(work_email, '@', 1)))
        end                                                           as work_email_local_part,
        case
            when personal_email is not null and position('@' in personal_email) > 0
                then lower(trim(split_part(personal_email, '@', 1)))
        end                                                           as personal_email_local_part,

        -- Employment context
        upper(trim(employment_status))                                as employment_status,
        upper(trim(employment_type))                                  as employment_type,
        trim(department)                                              as department,
        trim(job_title)                                               as job_title,
        trim(manager_hris_id)                                         as manager_hris_id,
        trim(location)                                                as location,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
