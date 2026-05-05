-- A non-HRIS source identity can auto-merge to at most one HRIS person across
-- all deterministic passes. Ambiguous candidates should be stewarded.

with auto_matches as (
    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified

    union all

    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_2_name_dob_hire') }}
    where auto_merge_qualified

    union all

    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_3_email_domain') }}
    where auto_merge_qualified
)

select
    source_record_key,
    count(distinct hris_person_key) as canonical_candidate_count
from auto_matches
group by source_record_key
having count(distinct hris_person_key) > 1
