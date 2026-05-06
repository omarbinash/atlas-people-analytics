-- Public People Analytics marts must not expose direct employee identifiers or
-- sensitive identity attributes. Core/intermediate models may keep these for
-- controlled joins; business-facing privacy marts may not.

select
    table_schema,
    table_name,
    column_name
from {{ target.database }}.information_schema.columns
where table_schema = upper('{{ target.schema }}_people_analytics')
  and lower(column_name) in (
      'canonical_person_id',
      'hris_person_key',
      'hris_employee_id',
      'employee_sk',
      'daily_workforce_key',
      'date_of_birth',
      'sin_last_4',
      'legal_first_name',
      'legal_last_name',
      'preferred_name',
      'legal_first_name_original',
      'legal_last_name_original',
      'preferred_name_original'
  )
