# Residual Review Report

This report summarizes review-only residual identity candidates produced
after deterministic dbt matching. These rows are steward-review aids,
not canonical truth and not automatic merges.

## Summary

| Metric | Value |
|---|---:|
| Stewardship rows sampled | 200 |
| Rows with at least one suggested candidate | 155 |
| Candidate rows emitted | 221 |
| Review yield rate | 77.5% |

## Recommendation Mix

| Recommendation | Candidates | Mean score | Mean evidence weight |
|---|---:|---:|---:|
| high_confidence_review | 53 | 0.996 | 1.000 |
| possible_review | 168 | 0.880 | 0.863 |

## Source-System Mix

| Source system | Candidates |
|---|---:|
| ATS | 217 |
| CRM | 4 |

## Feature Coverage

| Feature | Candidate coverage |
|---|---:|
| first_name_root | 79.2% |
| last_name | 79.2% |
| email_local | 100.0% |
| hire_date | 100.0% |
| deterministic_hint | 99.5% |

## Top Review Candidates

| Source record | Candidate canonical person | Recommendation | Score | Anchors | Reasons |
|---|---|---|---:|---:|---|
| CRM::crm_user_1075 | cp_8fb220970c18e301804344464edee09b | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| ATS::ats_836 | cp_8de205e4dd79e48bbfdbfd148c353684 | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| ATS::ats_704 | cp_3bd055746c66f9a8a0087aeabfba5361 | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| ATS::ats_4957 | cp_c09fa50fc154006146e6af4237bbf18f | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| ATS::ats_490 | cp_402fa3efafd266c878becbd6dee0cd1b | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| ATS::ats_4852 | cp_1148f73a387fd33303d4c89a4680c42a | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| ATS::ats_4809 | cp_e12770c3a54215699a798107e883a82d | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| ATS::ats_4765 | cp_917a1b2cb0ae06f44fc04a27be1ed06c | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| ATS::ats_4723 | cp_73e05d66bf2c4ec9cd40d0c858ae72c5 | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| ATS::ats_4608 | cp_11edd42272797221c8960b3f6a166deb | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| ATS::ats_4581 | cp_18323375074439b855511bf519303439 | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| ATS::ats_4514 | cp_857724e95446bd7ab72404124e5ea203 | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |

## Control Boundary

The residual engine is deliberately outside the canonical-person write path.
Any suggested match must still be adjudicated by a human steward before
it can influence canonical employee records or downstream marts.
