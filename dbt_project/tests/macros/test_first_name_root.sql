-- =============================================================================
-- test_first_name_root
-- =============================================================================
-- Singular test that exercises the first_name_root macro against the cases
-- enumerated in fixtures/first_name_root.yml.
--
-- The macro requires the caller to LEFT JOIN to the nickname_map seed; this
-- test does that join inline so the macro contract is exercised end-to-end.
--
-- Test passes when zero rows are returned. Each returned row is a failing
-- case with case_id, the input, the expected output, and the actual output.
-- =============================================================================

with cases (case_id, input_value, expected) as (
    select * from (values
        (1,  'Robert',     'robert'),
        (2,  'Bob',        'robert'),
        (3,  'Bobby',      'robert'),
        (4,  'Rob',        'robert'),
        (5,  'Liz',        'elizabeth'),
        (6,  'Beth',       'elizabeth'),
        (7,  'Raj',        'rajesh'),
        (8,  'Paco',       'francisco'),
        (9,  'Aiden',      'aiden'),
        (10, 'Steve',      'steve'),
        (11, '  BoB  ',    'robert'),
        (12, 'Mária',      'maria'),
        (13, '',           ''),
        (14, cast(null as varchar), cast(null as varchar))
    ) as t(case_id, input_value, expected)
),

cases_with_root as (
    select
        cases.case_id,
        cases.input_value,
        cases.expected,
        {{ first_name_root('cases.input_value') }} as actual
    from cases
    left join {{ ref('nickname_map') }} nm
        on nm.nickname = {{ normalize_name('cases.input_value') }}
)

select *
from cases_with_root
where coalesce(actual,   '__NULL__') != coalesce(expected, '__NULL__')
