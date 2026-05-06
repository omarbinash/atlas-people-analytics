-- No two HRIS employment spells may resolve to different canonical_person_ids
-- when they share DOB + work_email_local_part. This protects the rehire and
-- contractor-to-FTE invariants: new HRIS IDs should not fragment one person.

with resolved_hris_spells as (
    select
        hris.hris_employee_id,
        hris.date_of_birth,
        hris.work_email_local_part,
        canonical.canonical_person_id
    from {{ ref('stg_hris__employees') }} hris
    inner join {{ ref('int_hris_persons') }} persons
        on hris.date_of_birth = persons.date_of_birth
       and hris.personal_email_local_part = persons.personal_email_local_part
    inner join {{ ref('int_canonical_person') }} canonical
        on persons.hris_person_key = canonical.hris_person_key
    where hris.date_of_birth is not null
      and hris.work_email_local_part is not null
)

select
    date_of_birth,
    work_email_local_part,
    count(distinct hris_employee_id) as hris_employee_id_count,
    count(distinct canonical_person_id) as canonical_person_id_count
from resolved_hris_spells
group by date_of_birth, work_email_local_part
having count(distinct canonical_person_id) > 1
