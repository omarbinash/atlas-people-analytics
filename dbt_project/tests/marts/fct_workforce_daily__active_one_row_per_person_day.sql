-- Active headcount should never double-count one canonical person on one date.

select
    snapshot_date,
    canonical_person_id,
    count(*) as active_rows
from {{ ref('fct_workforce_daily') }}
where is_active_on_date
group by snapshot_date, canonical_person_id
having count(*) > 1
