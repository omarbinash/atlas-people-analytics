-- =============================================================================
-- test_normalize_name
-- =============================================================================
-- Singular test that exercises the normalize_name macro against the cases
-- enumerated in fixtures/normalize_name.yml. Mirror those cases here
-- exactly — if you add a case there, add it here in the same order.
--
-- Test passes when zero rows are returned. Each returned row is a failing
-- case with case_id, the input, the expected output, and the actual output.
-- =============================================================================

with cases (case_id, input_value, expected) as (
    select * from (values
        (1,  'Robert',       'robert'),
        (2,  '  Robert  ',   'robert'),
        (3,  'Édouard',      'edouard'),
        (4,  'Anaïs',        'anais'),
        (5,  'Mary-Jane',    'maryjane'),
        (6,  'O''Brien',     'obrien'),
        (7,  'Jean Paul',    'jeanpaul'),
        (8,  'Núñez',        'nunez'),
        (9,  'François',     'francois'),
        (10, '',             ''),
        (11, cast(null as varchar), cast(null as varchar)),
        (12, 'Robert3',      'robert'),
        (13, '张伟',         ''),
        (14, 'محمد',         ''),
        (15, 'Łukasz',       'lukasz')
    ) as t(case_id, input_value, expected)
),

results as (
    select
        case_id,
        input_value,
        expected,
        {{ normalize_name('input_value') }} as actual
    from cases
)

select *
from results
where coalesce(actual,   '__NULL__') != coalesce(expected, '__NULL__')
