-- The daily fact should not emit dates before hire or after the configured
-- snapshot as-of date/current_date.

{% if var('snapshot_as_of_date') %}
    {% set snapshot_as_of_expr = "to_date('" ~ var('snapshot_as_of_date') ~ "')" %}
{% else %}
    {% set snapshot_as_of_expr = "current_date()" %}
{% endif %}

select *
from {{ ref('fct_workforce_daily') }}
where snapshot_date < effective_start_date
   or snapshot_date > {{ snapshot_as_of_expr }}
