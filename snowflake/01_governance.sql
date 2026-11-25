-- =====================================================================
-- 01  GOVERNANCE: role hierarchy, schema grants, PII tag, masking policy
-- Run once, after 00_account_setup.sql.
-- =====================================================================

-- ---------- Roles ----------
-- Access roles hold privileges on objects; functional roles are job types
-- and are given access roles. sm_pii_reader holds no object privileges at
-- all: the masking policy checks for it to decide who sees real values.
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS sm_staging_rw;      -- access: read/write staging
CREATE ROLE IF NOT EXISTS sm_curated_ro;      -- access: read curated
CREATE ROLE IF NOT EXISTS sm_pii_reader;      -- access: see unmasked PII

CREATE ROLE IF NOT EXISTS sm_data_engineer;   -- functional
CREATE ROLE IF NOT EXISTS sm_analyst;         -- functional
CREATE ROLE IF NOT EXISTS sm_fraud_analyst;   -- functional
CREATE ROLE IF NOT EXISTS sm_pipeline         -- the Airflow pipeline (grants in 03)
    COMMENT = 'Runs the Sentinal Motor pipeline. Sees real PII to move it between layers.';

GRANT ROLE sm_staging_rw TO ROLE sm_data_engineer;
GRANT ROLE sm_curated_ro TO ROLE sm_data_engineer;
GRANT ROLE sm_curated_ro TO ROLE sm_analyst;
GRANT ROLE sm_curated_ro TO ROLE sm_fraud_analyst;
GRANT ROLE sm_pii_reader TO ROLE sm_fraud_analyst;    -- only fraud sees real PII

GRANT ROLE sm_data_engineer TO ROLE SYSADMIN;
GRANT ROLE sm_analyst       TO ROLE SYSADMIN;
-- sm_fraud_analyst and sm_pipeline are deliberately NOT rolled up to
-- SYSADMIN, so administrators do not inherit access to real PII.

-- ---------- Warehouse ----------
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE sm_data_engineer;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE sm_analyst;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE sm_fraud_analyst;

-- ---------- Schema grants (future grants survive table rebuilds) ----------
GRANT USAGE ON DATABASE sentinal_motor TO ROLE sm_curated_ro;
GRANT USAGE ON SCHEMA sentinal_motor.curated TO ROLE sm_curated_ro;
GRANT SELECT ON FUTURE TABLES IN SCHEMA sentinal_motor.curated TO ROLE sm_curated_ro;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA sentinal_motor.curated TO ROLE sm_curated_ro;

GRANT USAGE ON DATABASE sentinal_motor TO ROLE sm_staging_rw;
GRANT USAGE ON SCHEMA sentinal_motor.staging TO ROLE sm_staging_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA sentinal_motor.staging TO ROLE sm_staging_rw;
-- Nobody but SYSADMIN and sm_pipeline gets the RAW schema: its JSON holds
-- PII that a column masking policy cannot reach.

-- ---------- PII tag and tag-based masking policy ----------
USE ROLE SYSADMIN;

CREATE TAG IF NOT EXISTS sentinal_motor.governance.pii_type
    ALLOWED_VALUES 'name', 'ssn';

CREATE MASKING POLICY IF NOT EXISTS sentinal_motor.governance.mask_pii_string
AS (val STRING) RETURNS STRING ->
  CASE
    WHEN val IS NULL THEN NULL
    WHEN IS_ROLE_IN_SESSION('SM_PIPELINE') THEN val          -- pipeline must move real values
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('sentinal_motor.governance.pii_type') = 'name' THEN
         CASE WHEN IS_ROLE_IN_SESSION('SM_PII_READER') THEN val
              ELSE '*** MASKED ***'
         END
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('sentinal_motor.governance.pii_type') = 'ssn' THEN
         CASE WHEN IS_ROLE_IN_SESSION('SM_PII_READER') THEN 'XXX-XX-' || RIGHT(val, 4)
              ELSE '***-**-****'
         END
    ELSE '*** MASKED ***'                                    -- unknown tag value: fail closed
  END;

-- Every column tagged pii_type is now masked by this one policy.
-- Columns are tagged in 02_pipeline_objects.sql, when the tables exist.
-- To change the logic later use ALTER MASKING POLICY ... SET BODY:
-- a policy attached to a tag cannot be replaced.
ALTER TAG sentinal_motor.governance.pii_type
    SET MASKING POLICY sentinal_motor.governance.mask_pii_string;
