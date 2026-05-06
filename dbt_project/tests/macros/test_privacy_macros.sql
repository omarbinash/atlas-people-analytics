-- =============================================================================
-- test_privacy_macros
-- =============================================================================
-- Exercises the k-anonymity macros against the configured threshold.
-- Test passes when zero rows are returned.
-- =============================================================================

with cases (case_id, cohort_count, metric_value, expected_is_reportable) as (
    select * from (values
        (1, {{ var('k_anonymity_threshold') }} - 1, 100, false),
        (2, {{ var('k_anonymity_threshold') }}, 100, true),
        (3, {{ var('k_anonymity_threshold') }} + 1, 100, true)
    ) as t(case_id, cohort_count, metric_value, expected_is_reportable)
),

results as (
    select
        case_id,
        cohort_count,
        expected_is_reportable,
        {{ is_k_anonymous('cohort_count') }} as actual_is_reportable,
        {{ k_anonymize('metric_value', 'cohort_count', 'number(38, 0)') }} as anonymized_metric,
        {{ k_suppression_reason('cohort_count') }} as suppression_reason,
        {{ k_cohort_size_bucket('cohort_count') }} as cohort_size_bucket
    from cases
)

select *
from results
where actual_is_reportable != expected_is_reportable
   or (expected_is_reportable and anonymized_metric is null)
   or (not expected_is_reportable and anonymized_metric is not null)
   or (expected_is_reportable and suppression_reason is not null)
   or (not expected_is_reportable and suppression_reason != 'K_ANONYMITY_THRESHOLD')
