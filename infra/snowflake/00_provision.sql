-- =============================================================================
-- Atlas — Snowflake Provisioning Script
-- =============================================================================
-- This script creates a clean, isolated environment for the Atlas project:
--   - A dedicated database (ATLAS) with schemas for each pipeline tier
--   - A dedicated warehouse (ATLAS_WH) with auto-suspend to control cost
--   - A dedicated role (ATLAS_DEVELOPER) with least-privilege grants
--
-- Run as ACCOUNTADMIN.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- Warehouse: small, auto-suspend to keep credit usage minimal
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS ATLAS_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60          -- suspend after 60 seconds of inactivity
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Compute warehouse for the Atlas People Analytics project';

-- -----------------------------------------------------------------------------
-- Database and schemas (medallion architecture)
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ATLAS
    COMMENT = 'Atlas — canonical employee record + people analytics';

USE DATABASE ATLAS;

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Untransformed source-system mirrors. Loaded by seed pipeline.';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'dbt staging models — type-cast and renamed source mirrors.';

CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
    COMMENT = 'dbt intermediate models — identity resolution and matching.';

CREATE SCHEMA IF NOT EXISTS MARTS_CORE
    COMMENT = 'dbt marts — core dimensional models (dim_employee SCD2, facts).';

CREATE SCHEMA IF NOT EXISTS MARTS_PEOPLE
    COMMENT = 'dbt marts — People Analytics business-facing models.';

CREATE SCHEMA IF NOT EXISTS AUDIT
    COMMENT = 'Audit log for privacy-sensitive metric access.';

-- -----------------------------------------------------------------------------
-- Role: ATLAS_DEVELOPER (least-privilege for the pipeline)
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS ATLAS_DEVELOPER
    COMMENT = 'Developer role for the Atlas project — read/write on ATLAS DB only';

-- Warehouse usage
GRANT USAGE ON WAREHOUSE ATLAS_WH TO ROLE ATLAS_DEVELOPER;
GRANT OPERATE ON WAREHOUSE ATLAS_WH TO ROLE ATLAS_DEVELOPER;

-- Database-level
GRANT USAGE ON DATABASE ATLAS TO ROLE ATLAS_DEVELOPER;
GRANT CREATE SCHEMA ON DATABASE ATLAS TO ROLE ATLAS_DEVELOPER;

-- Schema-level — full access to all Atlas schemas
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.RAW TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.STAGING TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.INTERMEDIATE TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.MARTS_CORE TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.MARTS_PEOPLE TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.AUDIT TO ROLE ATLAS_DEVELOPER;

-- Future objects (so dbt can create new tables/views without re-granting)
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE ATLAS TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE ATLAS TO ROLE ATLAS_DEVELOPER;

-- -----------------------------------------------------------------------------
-- Assign the role to your user
-- -----------------------------------------------------------------------------
-- IMPORTANT: replace OMARBINASH with your Snowflake username if different
GRANT ROLE ATLAS_DEVELOPER TO USER OMARBINASH;

-- Make ATLAS_DEVELOPER the default role for that user (optional but convenient)
ALTER USER OMARBINASH SET DEFAULT_ROLE = ATLAS_DEVELOPER;
ALTER USER OMARBINASH SET DEFAULT_WAREHOUSE = ATLAS_WH;
ALTER USER OMARBINASH SET DEFAULT_NAMESPACE = ATLAS.RAW;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
-- After running this script, switch to the role and verify access:
--
--   USE ROLE ATLAS_DEVELOPER;
--   USE WAREHOUSE ATLAS_WH;
--   USE DATABASE ATLAS;
--   SHOW SCHEMAS;
--
-- You should see RAW, STAGING, INTERMEDIATE, MARTS_CORE, MARTS_PEOPLE, AUDIT,
-- plus the default INFORMATION_SCHEMA and PUBLIC.
