{{
    config(
        materialized='view',
        tags=['staging', 'crm']
    )
}}

-- =============================================================================
-- stg_crm__sales_reps
-- =============================================================================
-- 1:1 mirror of RAW_CRM_SALES_REPS.
-- Only sales/support roles have CRM accounts. Uses preferred name.
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_CRM_SALES_REPS') }}
),

renamed as (
    select
        trim(crm_user_id)                                             as crm_user_id,

        -- Names
        trim(preferred_first_name)                                    as preferred_first_name_original,
        trim(last_name)                                               as last_name_original,
        trim(display_name)                                            as display_name_original,
        lower(trim(preferred_first_name))                             as preferred_first_name,
        lower(trim(last_name))                                        as last_name,

        -- Email + local part
        lower(trim(crm_email))                                        as crm_email,
        case
            when crm_email is not null and position('@' in crm_email) > 0
                then lower(trim(split_part(crm_email, '@', 1)))
        end                                                           as crm_email_local_part,

        -- Org context
        trim(location_id)                                             as location_id,
        upper(trim(role))                                             as role,

        -- Lifecycle
        active                                                        as active,
        created_at                                                    as created_at,
        deactivated_at                                                as deactivated_at,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
