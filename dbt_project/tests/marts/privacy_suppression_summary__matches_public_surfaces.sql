-- The privacy suppression summary should agree with the public marts it
-- summarizes.

with expected as (
    select
        'workforce_headcount_daily' as privacy_surface,
        count(*) as row_count,
        count_if(is_reportable) as reportable_row_count,
        count_if(not is_reportable) as suppressed_row_count
    from {{ ref('workforce_headcount_daily') }}

    union all

    select
        'workforce_attrition_monthly' as privacy_surface,
        count(*) as row_count,
        count_if(is_reportable) as reportable_row_count,
        count_if(not is_reportable) as suppressed_row_count
    from {{ ref('workforce_attrition_monthly') }}
),

actual as (
    select
        privacy_surface,
        row_count,
        reportable_row_count,
        suppressed_row_count
    from {{ ref('privacy_suppression_summary') }}
)

select
    expected.privacy_surface,
    expected.row_count as expected_row_count,
    actual.row_count as actual_row_count,
    expected.reportable_row_count as expected_reportable_row_count,
    actual.reportable_row_count as actual_reportable_row_count,
    expected.suppressed_row_count as expected_suppressed_row_count,
    actual.suppressed_row_count as actual_suppressed_row_count
from expected
inner join actual
    on expected.privacy_surface = actual.privacy_surface
where expected.row_count != actual.row_count
   or expected.reportable_row_count != actual.reportable_row_count
   or expected.suppressed_row_count != actual.suppressed_row_count
