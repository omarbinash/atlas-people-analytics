-- dms_user_id must be unique across the unified set when present. This
-- enforces the bipartite-1:0..1 topology assumption documented in
-- int_dms_erp_unified.sql — if a real-world dataset ever introduces 1:N
-- (multiple ERP rows pointing to the same DMS), this test fails loudly
-- and we know to add aggregation logic before continuing.
select dms_user_id, count(*) as rows_with_this_dms_user_id
from {{ ref('int_dms_erp_unified') }}
where dms_user_id is not null
group by dms_user_id
having count(*) > 1
