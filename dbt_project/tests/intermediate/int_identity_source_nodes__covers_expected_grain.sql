-- int_identity_source_nodes must preserve the intended matching grain for each
-- upstream source/prep model. This catches accidental filters before the
-- canonical/queue coverage test runs.

with expected as (
    select 'HRIS' as source_system, count(*) as expected_count
    from {{ ref('int_hris_persons') }}

    union all

    select 'ATS' as source_system, count(*) as expected_count
    from {{ ref('stg_ats__candidates') }}

    union all

    select 'PAYROLL' as source_system, count(*) as expected_count
    from {{ ref('int_payroll_spells') }}

    union all

    select 'CRM' as source_system, count(*) as expected_count
    from {{ ref('stg_crm__sales_reps') }}

    union all

    select 'DMS_ERP' as source_system, count(*) as expected_count
    from {{ ref('int_dms_erp_unified') }}
),

actual as (
    select source_system, count(*) as actual_count
    from {{ ref('int_identity_source_nodes') }}
    group by source_system
)

select
    coalesce(expected.source_system, actual.source_system) as source_system,
    expected.expected_count,
    actual.actual_count
from expected
full outer join actual
    on expected.source_system = actual.source_system
where coalesce(expected.expected_count, -1) != coalesce(actual.actual_count, -1)
