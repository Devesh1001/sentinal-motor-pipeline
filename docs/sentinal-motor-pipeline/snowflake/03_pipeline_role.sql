-- =====================================================================
-- 03  PIPELINE ROLE GRANTS AND AIRFLOW SERVICE USER
-- Run after 02_pipeline_objects.sql (grants on ALL ... cover objects that exist).
-- =====================================================================

-- WHY BEFORE THE FIRST RUN: masking applies to every read, including the
-- pipeline's own reads. A role without PII access that runs the procedures
-- would copy '*** MASKED ***' into the next table as if it were real data,
-- and even SM_PII_READER only sees 'XXX-XX-1234'. So the pipeline gets its
-- own role, and the masking policy gives that role the real values.

USE ROLE SECURITYADMIN;

-- The role itself is created in 01_governance.sql (the masking policy refers to it).

GRANT USAGE ON WAREHOUSE compute_wh TO ROLE sm_pipeline;
GRANT USAGE ON DATABASE sentinal_motor TO ROLE sm_pipeline;
GRANT USAGE ON SCHEMA sentinal_motor.raw     TO ROLE sm_pipeline;
GRANT USAGE ON SCHEMA sentinal_motor.staging TO ROLE sm_pipeline;
GRANT USAGE ON SCHEMA sentinal_motor.curated TO ROLE sm_pipeline;

-- Raw: load files (COPY needs INSERT on the table, USAGE on stage and formats)
GRANT SELECT, INSERT ON ALL TABLES    IN SCHEMA sentinal_motor.raw TO ROLE sm_pipeline;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA sentinal_motor.raw TO ROLE sm_pipeline;
GRANT USAGE ON STAGE sentinal_motor.raw.s3_stage      TO ROLE sm_pipeline;
GRANT USAGE ON FILE FORMAT sentinal_motor.raw.ff_csv  TO ROLE sm_pipeline;
GRANT USAGE ON FILE FORMAT sentinal_motor.raw.ff_json TO ROLE sm_pipeline;

-- Staging and curated: read and write
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA sentinal_motor.staging TO ROLE sm_pipeline;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA sentinal_motor.staging TO ROLE sm_pipeline;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA sentinal_motor.curated TO ROLE sm_pipeline;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA sentinal_motor.curated TO ROLE sm_pipeline;
GRANT SELECT ON ALL VIEWS IN SCHEMA sentinal_motor.curated TO ROLE sm_pipeline;

-- Streams (reading a stream also needs SELECT on its table, granted above)
GRANT SELECT ON ALL STREAMS IN SCHEMA sentinal_motor.raw     TO ROLE sm_pipeline;
GRANT SELECT ON ALL STREAMS IN SCHEMA sentinal_motor.staging TO ROLE sm_pipeline;

-- Procedures
GRANT USAGE ON ALL PROCEDURES IN SCHEMA sentinal_motor.raw     TO ROLE sm_pipeline;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA sentinal_motor.staging TO ROLE sm_pipeline;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA sentinal_motor.curated TO ROLE sm_pipeline;

-- Analysts keep read access to the rebuilt curated tables and views
GRANT SELECT ON ALL TABLES IN SCHEMA sentinal_motor.curated TO ROLE sm_curated_ro;
GRANT SELECT ON ALL VIEWS  IN SCHEMA sentinal_motor.curated TO ROLE sm_curated_ro;

-- Service user for Airflow: TYPE = SERVICE means no password login at all,
-- only key-pair authentication.
CREATE USER IF NOT EXISTS sentinal_pipeline_svc
    TYPE = SERVICE
    DEFAULT_ROLE = sm_pipeline
    DEFAULT_WAREHOUSE = compute_wh
    COMMENT = 'Airflow service user for the Sentinal Motor pipeline';
GRANT ROLE sm_pipeline TO USER sentinal_pipeline_svc;

-- Paste your PUBLIC key here (the text between the BEGIN/END lines of
-- rsa_key.pub, joined into one line). Never paste the private key.
ALTER USER sentinal_pipeline_svc SET RSA_PUBLIC_KEY = '<paste public key here>';
DESC USER sentinal_pipeline_svc;      -- RSA_PUBLIC_KEY_FP should now have a value

-- Optional, for testing the procedures yourself in a worksheet.
-- Revoke it when testing is finished.
-- GRANT ROLE sm_pipeline TO USER <your_user_name>;
