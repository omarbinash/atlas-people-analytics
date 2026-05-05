{{
    config(
        materialized='view',
        tags=['staging', 'erp']
    )
}}

-- =============================================================================
-- stg_erp__users
-- =============================================================================
-- 1:1 mirror of RAW_ERP_USERS.
-- ERP mostly mirrors DMS but ~10% of LINKED_DMS_USER_ID values are NULL,
-- modeling real-world drift where the DMS-ERP link is broken.
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_ERP_USERS') }}
),

renamed as (
    select
        trim(erp_user_id)                                             as erp_user_id,
        trim(linked_dms_user_id)                                      as linked_dms_user_id,

        -- Names
        trim(short_first_name)                                        as short_first_name_original,
        trim(last_name)                                               as last_name_original,
        lower(trim(short_first_name))                                 as short_first_name,
        lower(trim(last_name))                                        as last_name,

        -- Email + local part
        lower(trim(erp_email))                                        as erp_email,
        case
            when erp_email is not null and position('@' in erp_email) > 0
                then lower(trim(split_part(erp_email, '@', 1)))
        end                                                           as erp_email_local_part,

        -- Permissions context
        trim(role_code)                                               as role_code,
        trim(permissions_group)                                       as permissions_group,

        -- Lifecycle / activity
        created_at                                                    as created_at,
        last_login_at                                                 as last_login_at,

        -- Useful flag for downstream: is this row missing its DMS link?
        case when linked_dms_user_id is null then true else false end as has_broken_dms_link,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
