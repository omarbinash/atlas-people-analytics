-- Every source identity node must end in exactly one of two places:
--   * HRIS nodes appear in int_canonical_person
--   * non-HRIS nodes are either auto-matched into canonical output or queued
--     for stewardship

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
),

queue as (
    select distinct source_record_key
    from {{ ref('int_stewardship_queue') }}
),

hris_orphans as (
    select
        nodes.source_system,
        nodes.source_record_key,
        'HRIS_MISSING_FROM_CANONICAL' as orphan_reason
    from {{ ref('int_identity_source_nodes') }} nodes
    left join {{ ref('int_canonical_person') }} canonical
        on nodes.hris_person_key = canonical.hris_person_key
    where nodes.source_system = 'HRIS'
      and canonical.hris_person_key is null
),

non_hris_orphans as (
    select
        nodes.source_system,
        nodes.source_record_key,
        'NON_HRIS_MISSING_FROM_CANONICAL_AND_QUEUE' as orphan_reason
    from {{ ref('int_identity_source_nodes') }} nodes
    left join auto_matches
        on nodes.source_record_key = auto_matches.source_record_key
    left join queue
        on nodes.source_record_key = queue.source_record_key
    where nodes.source_system != 'HRIS'
      and auto_matches.source_record_key is null
      and queue.source_record_key is null
),

double_assigned as (
    select
        nodes.source_system,
        nodes.source_record_key,
        'NON_HRIS_IN_BOTH_CANONICAL_AND_QUEUE' as orphan_reason
    from {{ ref('int_identity_source_nodes') }} nodes
    inner join auto_matches
        on nodes.source_record_key = auto_matches.source_record_key
    inner join queue
        on nodes.source_record_key = queue.source_record_key
    where nodes.source_system != 'HRIS'
)

select * from hris_orphans
union all
select * from non_hris_orphans
union all
select * from double_assigned
