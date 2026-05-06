{#-
============================================================================
  match_confidence — three macros that codify the locked Phase 2C scoring
============================================================================

  These macros emit SQL expressions that compute, per candidate match:

    1. match_score          — additive confidence in [0.0, 1.0]
    2. match_anchor_count   — count of medium-or-stronger independent anchors
    3. auto_merge_qualified — boolean: passes the >=0.95 score AND >=2 anchor
                              floor (or is a Pass-0 structural FK)

  Anchors and weights below mirror the locked table in
  `~/.claude/.../memory/phase_2c_anchor_table.md`. Edit weights ONLY by
  updating the memory and the int_match_audit_log doc together — the
  numbers are load-bearing and every change should be defensible.

  Anchor weights:
    +0.40  work_email_local_part exact         strong
    +0.35  personal_email_local_part exact     strong
    +0.30  DOB exact                           strong
    +0.20  EMPLOYEE_PAYROLL_ID continuity      medium (intra-payroll only)
    +0.20  last_name + first_name_root match   weak-medium (counts as 1 anchor)
    +0.20  hire_date exact                     medium
    +0.10  hire_date within +/- 7 days         weak
    +0.05  hire_date within +/- 30 days        very weak (does NOT count toward
                                                          independent anchor floor)

  Special case: structural FK match (Pass 0 ERP -> DMS via linked_dms_user_id)
  short-circuits to score = 1.0 and bypasses the anchor count check, because
  it is a deterministic FK rather than a probabilistic match.

  Caller usage — pass the BOOLEAN EXPRESSION as a string for each anchor that
  applies. Anchors that don't apply for a given pass can be omitted (they
  default to 'false'):

    select
      ...,
      {{ match_score(
          work_email_match='src.work_email_local_part = tgt.work_email_local_part',
          name_match='src.first_name_root = tgt.first_name_root and src.last_name = tgt.last_name',
          hire_date_exact='src.hire_date = tgt.hire_date'
      ) }} as match_score,
      {{ match_anchor_count(
          work_email_match='src.work_email_local_part = tgt.work_email_local_part',
          name_match='src.first_name_root = tgt.first_name_root and src.last_name = tgt.last_name',
          hire_date_exact='src.hire_date = tgt.hire_date'
      ) }} as match_anchor_count

  Yes, the same expressions get repeated. The alternative is a single macro
  that emits multiple columns at once — possible via {%- set ... -%} in a
  parent CTE, but it sacrifices SQL readability. The repetition is the
  honest trade.
-#}


{#- =====================================================================
    match_score
========================================================================= -#}

{% macro match_score(
    work_email_match='false',
    personal_email_match='false',
    dob_match='false',
    payroll_id_continuity='false',
    name_match='false',
    hire_date_exact='false',
    hire_date_within_7d='false',
    hire_date_within_30d='false',
    structural_fk_match='false'
) -%}
case
    when ({{ structural_fk_match }}) then 1.0
    else least(1.0,
        (case when ({{ work_email_match }})       then 0.40 else 0 end)
      + (case when ({{ personal_email_match }})   then 0.35 else 0 end)
      + (case when ({{ dob_match }})              then 0.30 else 0 end)
      + (case when ({{ payroll_id_continuity }})  then 0.20 else 0 end)
      + (case when ({{ name_match }})             then 0.20 else 0 end)
      + (case when ({{ hire_date_exact }})        then 0.20
              when ({{ hire_date_within_7d }})    then 0.10
              when ({{ hire_date_within_30d }})   then 0.05
              else 0 end)
    )
end
{%- endmacro %}


{#- =====================================================================
    match_anchor_count
    Counts only medium-or-stronger independent anchors. hire_date +/- 30d
    alone does NOT count toward this — it's too weak (matches ~10% of
    population for any given target hire_date).
========================================================================= -#}

{% macro match_anchor_count(
    work_email_match='false',
    personal_email_match='false',
    dob_match='false',
    payroll_id_continuity='false',
    name_match='false',
    hire_date_exact='false',
    hire_date_within_7d='false',
    hire_date_within_30d='false'
) -%}
(case when ({{ work_email_match }})      then 1 else 0 end)
+ (case when ({{ personal_email_match }})  then 1 else 0 end)
+ (case when ({{ dob_match }})             then 1 else 0 end)
+ (case when ({{ payroll_id_continuity }}) then 1 else 0 end)
+ (case when ({{ name_match }})            then 1 else 0 end)
+ (case when ({{ hire_date_exact }}) or ({{ hire_date_within_7d }}) then 1 else 0 end)
{%- endmacro %}


{#- =====================================================================
    auto_merge_qualified
    The locked auto-merge rule, in one place. Returns a boolean
    expression suitable for use in a CASE or WHERE clause.
========================================================================= -#}

{% macro auto_merge_qualified(score_col, anchor_count_col, structural_fk_col='false') -%}
(({{ structural_fk_col }}) or ({{ score_col }} >= 0.95 and {{ anchor_count_col }} >= 2))
{%- endmacro %}
