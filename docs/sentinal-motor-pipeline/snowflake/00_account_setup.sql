-- =====================================================================
-- 00  ACCOUNT SETUP: database, schemas, S3 storage integration,
--     external stage and file formats
-- Replace every <placeholder> before running. Run once.
-- =====================================================================

-- ---------- Database and schemas (owned by SYSADMIN) ----------
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS sentinal_motor;
CREATE SCHEMA IF NOT EXISTS sentinal_motor.raw;          -- files exactly as received
CREATE SCHEMA IF NOT EXISTS sentinal_motor.staging;      -- typed, validated, deduplicated
CREATE SCHEMA IF NOT EXISTS sentinal_motor.curated;      -- star schema + reporting views
CREATE SCHEMA IF NOT EXISTS sentinal_motor.governance;   -- tags and masking policies

-- ---------- Storage integration (needs ACCOUNTADMIN) ----------
-- Snowflake assumes an IAM role in your AWS account; no AWS keys are stored.
-- See aws/iam/ for the role's permission and trust policies.
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS s3_sentinal_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws_account_id>:role/snowflake-sentinal-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>/sentinal_motor/');

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID into the IAM
-- role's trust policy (aws/iam/snowflake-trust-policy.example.json).
-- Do not CREATE OR REPLACE the integration afterwards: that generates a
-- new external ID and breaks the trust policy.
DESC INTEGRATION s3_sentinal_int;

GRANT USAGE ON INTEGRATION s3_sentinal_int TO ROLE SYSADMIN;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE SYSADMIN;
GRANT APPLY TAG ON ACCOUNT TO ROLE SYSADMIN;

-- ---------- Stage and file formats (owned by SYSADMIN) ----------
USE ROLE SYSADMIN;

CREATE FILE FORMAT IF NOT EXISTS sentinal_motor.raw.ff_json
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;                  -- one row per array element, not one per file

CREATE FILE FORMAT IF NOT EXISTS sentinal_motor.raw.ff_csv
  TYPE = CSV
  PARSE_HEADER = TRUE                        -- needed to match columns by name
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE     -- table has 3 extra metadata columns
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE;

CREATE STAGE IF NOT EXISTS sentinal_motor.raw.s3_stage
  STORAGE_INTEGRATION = s3_sentinal_int
  URL = 's3://<bucket>/sentinal_motor/';

-- Should list the files the generator uploaded
LIST @sentinal_motor.raw.s3_stage;
