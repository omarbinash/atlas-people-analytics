{#-
============================================================================
  first_name_root
============================================================================
  Returns the canonical (legal) first name for a given input first name,
  collapsing common nicknames to their root form. e.g. 'Bob' -> 'robert',
  'Liz' -> 'elizabeth'. If the input is not a known nickname, returns the
  normalize_name'd literal (passthrough).

  Spec: fixtures/first_name_root.yml
  Test: tests/macros/test_first_name_root.sql

  CALLER REQUIREMENT: this macro emits a column expression that depends on
  a LEFT JOIN to seeds.nickname_map being present in the same SELECT. The
  caller must include:

      LEFT JOIN {{ ref('nickname_map') }} {{ alias }}
          ON {{ alias }}.nickname = {{ normalize_name(input_col) }}

  ...where `input_col` matches the column passed to first_name_root and
  `alias` matches the second arg (default 'nm').

  This couples the macro to the join, which is intentional — it forces the
  caller to make the dependency explicit in the model SQL rather than
  hiding it in a correlated subquery (which would be slow at scale).

  Ambiguous nicknames (steve, alex, sam, chris, charlie, ed, andy, mo, pat,
  rick, frank) are deliberately omitted from nickname_map. For those, the
  COALESCE falls back to the normalized literal — so 'Steve' returns
  'steve', not 'steven' or 'stephen'. Cross-resolving those cases requires
  an independent anchor (email, DOB) in the matcher's pass logic, or the
  record routes to stewardship. See seeds/_seeds.yml for the full exclusion
  policy.
-#}

{% macro first_name_root(input_col, alias='nm') -%}
coalesce({{ alias }}.canonical_first_name, {{ normalize_name(input_col) }})
{%- endmacro %}
