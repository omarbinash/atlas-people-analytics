-- Every row in the unified DMS+ERP set must have at least one source PK.
-- A row with both dms_user_id and erp_user_id NULL would be a FULL OUTER JOIN
-- artifact and indicates a bug in the merge.
select dms_erp_person_key, dms_user_id, erp_user_id, merge_topology
from {{ ref('int_dms_erp_unified') }}
where dms_user_id is null and erp_user_id is null
