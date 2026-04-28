-- =============================================================================
-- Atlas — Raw Source Tables
-- =============================================================================
-- Five source systems, each with a different name representation strategy
-- (the realistic mess we are about to model and resolve).
--
-- Run as ATLAS_DEVELOPER after 00_provision.sql.
-- =============================================================================

USE ROLE ATLAS_DEVELOPER;
USE WAREHOUSE ATLAS_WH;
USE DATABASE ATLAS;
USE SCHEMA RAW;

-- -----------------------------------------------------------------------------
-- HRIS (BambooHR-shape) — legal first + last name, source of truth for employment
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_HRIS_EMPLOYEES (
    HRIS_EMPLOYEE_ID         VARCHAR(32)   NOT NULL,
    LEGAL_FIRST_NAME         VARCHAR(100)  NOT NULL,
    LEGAL_LAST_NAME          VARCHAR(100)  NOT NULL,
    PREFERRED_NAME           VARCHAR(100),
    DATE_OF_BIRTH            DATE,
    PERSONAL_EMAIL           VARCHAR(255),
    WORK_EMAIL               VARCHAR(255),
    HIRE_DATE                DATE          NOT NULL,
    TERMINATION_DATE         DATE,
    EMPLOYMENT_STATUS        VARCHAR(32)   NOT NULL,  -- ACTIVE | TERMINATED | ON_LEAVE
    EMPLOYMENT_TYPE          VARCHAR(32),             -- FTE | CONTRACTOR | PART_TIME
    DEPARTMENT               VARCHAR(100),
    JOB_TITLE                VARCHAR(150),
    MANAGER_HRIS_ID          VARCHAR(32),
    LOCATION                 VARCHAR(100),
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- ATS (Greenhouse-shape) — preferred name, only knows people from hire to start date
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_ATS_CANDIDATES (
    ATS_CANDIDATE_ID         VARCHAR(32)   NOT NULL,
    PREFERRED_FIRST_NAME     VARCHAR(100)  NOT NULL,
    LAST_NAME                VARCHAR(100)  NOT NULL,
    EMAIL                    VARCHAR(255),
    PHONE                    VARCHAR(32),
    APPLICATION_DATE         DATE,
    OFFER_ACCEPTED_DATE      DATE,
    SOURCED_FROM             VARCHAR(64),
    REQUISITION_DEPARTMENT   VARCHAR(100),
    REQUISITION_JOB_TITLE    VARCHAR(150),
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- Payroll (ADP-shape) — legal name, includes SIN/last-4 (sensitive!)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_PAYROLL_RECORDS (
    PAYROLL_RECORD_ID        VARCHAR(32)   NOT NULL,
    EMPLOYEE_PAYROLL_ID      VARCHAR(32)   NOT NULL,    -- payroll's own ID, separate from HRIS
    LEGAL_FIRST_NAME         VARCHAR(100)  NOT NULL,
    LEGAL_LAST_NAME          VARCHAR(100)  NOT NULL,
    SIN_LAST_4               VARCHAR(4),                -- last 4 only (sensitive)
    PAY_PERIOD_START         DATE          NOT NULL,
    PAY_PERIOD_END           DATE          NOT NULL,
    GROSS_AMOUNT_CAD         NUMBER(12,2),
    HOURS_WORKED             NUMBER(8,2),
    JOB_CODE                 VARCHAR(64),
    COST_CENTER              VARCHAR(64),
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- CRM (Dabadu-shape) — preferred name, captures sales-floor activity
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_CRM_SALES_REPS (
    CRM_USER_ID              VARCHAR(32)   NOT NULL,
    PREFERRED_FIRST_NAME     VARCHAR(100)  NOT NULL,
    LAST_NAME                VARCHAR(100)  NOT NULL,
    DISPLAY_NAME             VARCHAR(200),              -- often "First Last" but spelled idiosyncratically
    CRM_EMAIL                VARCHAR(255),
    LOCATION_ID              VARCHAR(32),
    ROLE                     VARCHAR(64),               -- SALES_REP | F_AND_I | SERVICE_ADVISOR | MANAGER
    ACTIVE                   BOOLEAN       NOT NULL,
    CREATED_AT               TIMESTAMP_NTZ,
    DEACTIVATED_AT           TIMESTAMP_NTZ,
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- DMS (PBS-shape) — shortened first name, deal flow + commissions
-- This is the system that mirrors into the ERP
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_DMS_USERS (
    DMS_USER_ID              VARCHAR(32)   NOT NULL,
    SHORT_FIRST_NAME         VARCHAR(50)   NOT NULL,    -- truncated/abbreviated
    LAST_NAME                VARCHAR(100)  NOT NULL,
    DMS_USERNAME             VARCHAR(64),               -- system login
    LOCATION_CODE            VARCHAR(16),
    DEPARTMENT_CODE          VARCHAR(16),               -- NEW | USED | F&I | SVC | PARTS | BDC
    HIRE_DATE_DMS            DATE,                      -- often differs slightly from HRIS hire date
    TERMINATED_DATE_DMS      DATE,
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- ERP (custom) — mirrors DMS but adds an internal user_id and audit trail
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_ERP_USERS (
    ERP_USER_ID              VARCHAR(32)   NOT NULL,
    LINKED_DMS_USER_ID       VARCHAR(32),               -- foreign-key into DMS, sometimes NULL
    SHORT_FIRST_NAME         VARCHAR(50)   NOT NULL,    -- pulled from DMS
    LAST_NAME                VARCHAR(100)  NOT NULL,
    ERP_EMAIL                VARCHAR(255),
    ROLE_CODE                VARCHAR(32),
    PERMISSIONS_GROUP        VARCHAR(64),
    CREATED_AT               TIMESTAMP_NTZ,
    LAST_LOGIN_AT            TIMESTAMP_NTZ,
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SHOW TABLES IN SCHEMA ATLAS.RAW;
