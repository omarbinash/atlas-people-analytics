-- erp_user_id must be unique across the unified set when present. ERP rows
-- always have a unique erp_user_id; the FULL OUTER JOIN should preserve that
-- uniqueness. Failure here means the join produced fan-out, which violates
-- the assumption that each ERP row points to at most one DMS row.
select erp_user_id, count(*) as rows_with_this_erp_user_id
from {{ ref('int_dms_erp_unified') }}
where erp_user_id is not null
group by erp_user_id
having count(*) > 1
