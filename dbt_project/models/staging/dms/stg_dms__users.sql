{{
    config(
        materialized='view',
        tags=['staging', 'dms']
    )
}}

-- =============================================================================
-- stg_dms__users
-- =============================================================================
-- 1:1 mirror of RAW_DMS_USERS.
-- DMS uses the SHORTENED first name (key drift point: 'Bob' for 'Robert').
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_DMS_USERS') }}
),

renamed as (
    select
        trim(dms_user_id)                                             as dms_user_id,

        -- Names
        trim(short_first_name)                                        as short_first_name_original,
        trim(last_name)                                               as last_name_original,
        lower(trim(short_first_name))                                 as short_first_name,
        lower(trim(last_name))                                        as last_name,

        lower(trim(dms_username))                                     as dms_username,

        -- Org context
        trim(location_code)                                           as location_code,
        trim(department_code)                                         as department_code,

        -- Lifecycle (note: hire_date here can drift ±1-3 days from HRIS.HIRE_DATE)
        hire_date_dms                                                 as hire_date_dms,
        terminated_date_dms                                           as terminated_date_dms,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
