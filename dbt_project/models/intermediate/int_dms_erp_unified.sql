{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'pass_0']
    )
}}

-- =============================================================================
-- int_dms_erp_unified — Phase 2C, Pass 0 (structural FK merge)
-- =============================================================================
-- Graph-merge of DMS users and ERP users via the hard FK
-- ERP.LINKED_DMS_USER_ID -> DMS.DMS_USER_ID. One row per DMS+ERP person,
-- where "person" = a connected component in the bipartite (DMS, ERP) graph.
--
-- Three topologies in this dataset:
--
--   DMS_AND_ERP (~90%):       DMS user with linked ERP user
--   DMS_ONLY:                 DMS user with no ERP account
--   ERP_ONLY_BROKEN_LINK:     ERP user whose linked_dms_user_id is NULL
--                             (~10% of ERP rows — modeled drift in the synth)
--
-- Pass 0 is structural, not probabilistic. Matched (dms_user_id, erp_user_id)
-- pairs get confidence = 1.0 in match_confidence and bypass the >=2-anchor
-- floor — the FK is deterministic. Downstream passes trust this linkage when
-- joining the unified DMS+ERP person to HRIS / ATS / payroll / CRM.
--
-- After this model, DMS and ERP do NOT appear separately in any Phase 2C
-- intermediate. The unified identity is the unit of work.
--
-- ---------------------------------------------------------------------------
-- Topology assumption: each DMS user has at most one ERP user pointing back
-- via linked_dms_user_id. Holds in this synthesizer (1:0..1 per person). If
-- real-world data introduces 1:N (rare — multiple ERP accounts per DMS user),
-- the unique tests on dms_user_id will fail loudly. That's the right
-- behavior — we want to surface the assumption violation, not silently
-- aggregate. A v2 could group by dms_user_id and array_agg the erp_user_ids;
-- defer that until a real case appears.
-- ---------------------------------------------------------------------------
--
-- Output schema:
--   dms_erp_person_key       Surrogate key over (dms_user_id, erp_user_id)
--   dms_user_id              Nullable (NULL for ERP_ONLY_BROKEN_LINK)
--   erp_user_id              Nullable (NULL for DMS_ONLY)
--   merge_topology           'DMS_AND_ERP' | 'DMS_ONLY' | 'ERP_ONLY_BROKEN_LINK'
--   has_dms / has_erp        Booleans for filter convenience
--   has_broken_link          TRUE iff has_erp AND erp.linked_dms_user_id IS NULL
--   short_first_name         DMS preferred, ERP fallback (lowercase, trimmed)
--   last_name                DMS preferred, ERP fallback (lowercase, trimmed)
--   short_first_name_original   Original casing for display
--   last_name_original          Original casing for display
--   dms_username             DMS-only (NULL when has_dms = false)
--   erp_email                ERP-only
--   erp_email_local_part     ERP-only — Pass 1 anchor against HRIS work email
--   ... + DMS-only and ERP-only org/lifecycle context preserved
-- =============================================================================

with dms as (
    select * from {{ ref('stg_dms__users') }}
),

erp as (
    select * from {{ ref('stg_erp__users') }}
),

merged as (
    select
        -- ---- Identifiers ----
        dms.dms_user_id                                                          as dms_user_id,
        erp.erp_user_id                                                          as erp_user_id,
        erp.linked_dms_user_id                                                   as erp_linked_dms_user_id,

        -- ---- Topology classification ----
        case
            when dms.dms_user_id is not null and erp.erp_user_id is not null then 'DMS_AND_ERP'
            when dms.dms_user_id is not null and erp.erp_user_id is null     then 'DMS_ONLY'
            when dms.dms_user_id is null     and erp.erp_user_id is not null then 'ERP_ONLY_BROKEN_LINK'
        end                                                                      as merge_topology,

        case when dms.dms_user_id is not null then true else false end           as has_dms,
        case when erp.erp_user_id is not null then true else false end           as has_erp,
        case
            when erp.erp_user_id is not null and erp.linked_dms_user_id is null
                then true else false
        end                                                                      as has_broken_link,

        -- ---- Identity attributes (DMS preferred, ERP fallback) ----
        -- DMS is the system of record for short-name + last-name when both
        -- exist; ERP is a downstream copy that drifts. Prefer DMS.
        coalesce(dms.short_first_name,           erp.short_first_name)           as short_first_name,
        coalesce(dms.last_name,                  erp.last_name)                  as last_name,
        coalesce(dms.short_first_name_original,  erp.short_first_name_original)  as short_first_name_original,
        coalesce(dms.last_name_original,         erp.last_name_original)         as last_name_original,

        -- ---- DMS-only fields ----
        dms.dms_username                                                         as dms_username,
        dms.location_code                                                        as dms_location_code,
        dms.department_code                                                      as dms_department_code,
        dms.hire_date_dms                                                        as hire_date_dms,
        dms.terminated_date_dms                                                  as terminated_date_dms,

        -- ---- ERP-only fields ----
        erp.erp_email                                                            as erp_email,
        erp.erp_email_local_part                                                 as erp_email_local_part,
        erp.role_code                                                            as erp_role_code,
        erp.permissions_group                                                    as erp_permissions_group,
        erp.created_at                                                           as erp_created_at,
        erp.last_login_at                                                        as erp_last_login_at,

        -- ---- Provenance ----
        greatest(
            coalesce(dms.loaded_at, erp.loaded_at),
            coalesce(erp.loaded_at, dms.loaded_at)
        )                                                                        as loaded_at,
        '{{ invocation_id }}'                                                    as _dbt_invocation_id

    from dms
    full outer join erp
        on erp.linked_dms_user_id = dms.dms_user_id
),

keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['dms_user_id', 'erp_user_id']) }} as dms_erp_person_key,
        merged.*
    from merged
)

select * from keyed
