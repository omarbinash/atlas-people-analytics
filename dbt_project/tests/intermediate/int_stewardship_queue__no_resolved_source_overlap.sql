-- Stewardship should contain only source identities that failed deterministic
-- auto-merge. If a source_record_key appears here and in auto matches, the
-- matcher has created two conflicting outcomes for one input.

with auto_matches as (
    select distinct source_record_key
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified

    union

    select distinct source_record_key
    from {{ ref('int_identity_pass_2_name_dob_hire') }}
    where auto_merge_qualified

    union

    select distinct source_record_key
    from {{ ref('int_identity_pass_3_email_domain') }}
    where auto_merge_qualified
)

select queue.source_record_key
from {{ ref('int_stewardship_queue') }} queue
inner join auto_matches
    on queue.source_record_key = auto_matches.source_record_key
