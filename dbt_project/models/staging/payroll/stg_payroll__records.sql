{{
    config(
        materialized='view',
        tags=['staging', 'payroll']
    )
}}

-- =============================================================================
-- stg_payroll__records
-- =============================================================================
-- 1:1 mirror of RAW_PAYROLL_RECORDS.
-- Note: SIN_LAST_4 is sensitive — restrict access at the marts layer.
-- Payroll often LAGS HRIS on name updates (e.g. marriage), so the same
-- person may appear here under their pre-marriage name long after HRIS updated.
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_PAYROLL_RECORDS') }}
),

renamed as (
    select
        trim(payroll_record_id)                                       as payroll_record_id,
        trim(employee_payroll_id)                                     as employee_payroll_id,

        -- Names (keep original casing in *_original for display)
        trim(legal_first_name)                                        as legal_first_name_original,
        trim(legal_last_name)                                         as legal_last_name_original,
        lower(trim(legal_first_name))                                 as legal_first_name,
        lower(trim(legal_last_name))                                  as legal_last_name,

        -- Sensitive: surface but flag in column docs
        sin_last_4                                                    as sin_last_4,

        -- Period and amounts
        pay_period_start                                              as pay_period_start,
        pay_period_end                                                as pay_period_end,
        gross_amount_cad                                              as gross_amount_cad,
        hours_worked                                                  as hours_worked,

        -- Org context
        trim(job_code)                                                as job_code,
        trim(cost_center)                                             as cost_center,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
