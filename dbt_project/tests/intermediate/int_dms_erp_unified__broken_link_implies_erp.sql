-- has_broken_link should only fire when has_erp = TRUE. The "broken link"
-- topology is specifically an ERP user with NULL linked_dms_user_id; it
-- cannot exist for a DMS-only or paired row.
select dms_erp_person_key, has_dms, has_erp, has_broken_link, merge_topology
from {{ ref('int_dms_erp_unified') }}
where has_broken_link = true and has_erp = false
