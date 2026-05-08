{#-
============================================================================
  normalize_name
============================================================================
  Lowercase + accent-strip + non-alpha-strip transformation for cross-source
  name matching. Pure SQL - no Python UDFs, no extensions.

  Implementation: TRIM -> TRANSLATE (Latin-1 + Latin Extended-A folding) ->
  LOWER -> REGEXP_REPLACE non-[a-z].

  Spec: fixtures/normalize_name.yml
  Test: tests/macros/test_normalize_name.sql
  Python equivalent: seeds/name_strategies.py::normalize_name_for_matching

  Coverage: French, Spanish, Italian, Portuguese, Polish, Czech, Croatian
  diacritics. Synthesizer locales (en_CA, fr_CA, en_IN, es_MX) all covered.

  Non-Latin scripts (zh_CN, ar_AA): returns empty string after non-alpha
  strip. The matcher relies on email-domain anchors (Pass 3) for cross-script
  identity resolution.

  Tradeoff: pure SQL keeps the warehouse self-contained and the function
  inlinable for query optimization. Python UDF using `unidecode` would
  give full Unicode transliteration but adds Snowpark infrastructure and
  costs more per row. Revisit if future residual matching needs CJK name roots.
-#}

{% macro normalize_name(col) -%}
regexp_replace(
    lower(
        translate(
            trim({{ col }}),
            'ÀÁÂÃÄÅàáâãäåÈÉÊËèéêëÌÍÎÏìíîïÒÓÔÕÖØòóôõöøÙÚÛÜùúûüÝýÿÇçÑñŁłŚśŠšŹźŻżŽžĐđŘřŤť',
            'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOOooooooUUUUuuuuYyyCcNnLlSsSsZzZzZzDdRrTt'
        )
    ),
    '[^a-z]',
    ''
)
{%- endmacro %}
