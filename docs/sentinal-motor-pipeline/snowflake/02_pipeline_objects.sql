-- =====================================================================
-- 02  PIPELINE OBJECTS: raw, staging and curated tables, streams,
--     stored procedures, reporting views and PII tags
-- Run as SYSADMIN after 00_account_setup.sql and 01_governance.sql.
-- Re-running this file RECREATES the tables and streams (data is lost).
-- =====================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE compute_wh;
USE DATABASE sentinal_motor;


-- =====================================================================
-- 5.4  RAW TABLES, STREAMS AND THE S3 LOAD PROCEDURE
-- =====================================================================

-- Raw = exactly what arrived, plus where it came from.
-- Policies (CSV): every column STRING, so a bad value like 'N/A' still lands.
CREATE OR REPLACE TABLE raw.policies_raw (
    policy_id          STRING,
    holder_name        STRING,
    ssn                STRING,
    state              STRING,
    vehicle_id         STRING,
    premium_amount     STRING,
    policy_start_date  STRING,
    policy_end_date    STRING,
    updated_at         STRING,
    file_name          STRING,
    file_row_number    NUMBER,
    loaded_at          TIMESTAMP_LTZ
);

-- Claims and telematics (JSON): one VARIANT column holds each record.
CREATE OR REPLACE TABLE raw.claims_raw (
    payload          VARIANT,
    file_name        STRING,
    file_row_number  NUMBER,
    loaded_at        TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE raw.telematics_raw (
    payload          VARIANT,
    file_name        STRING,
    file_row_number  NUMBER,
    loaded_at        TIMESTAMP_LTZ
);

-- Streams: a bookmark on each raw table. They return only rows added since
-- the last time a DML statement read them. APPEND_ONLY because raw is
-- insert-only. Created BEFORE the first load, so the first load is captured.
CREATE OR REPLACE STREAM raw.policies_raw_stream   ON TABLE raw.policies_raw   APPEND_ONLY = TRUE;
CREATE OR REPLACE STREAM raw.claims_raw_stream     ON TABLE raw.claims_raw     APPEND_ONLY = TRUE;
CREATE OR REPLACE STREAM raw.telematics_raw_stream ON TABLE raw.telematics_raw APPEND_ONLY = TRUE;

-- Load procedure: COPY INTO skips files it has already loaded (64-day
-- memory per table), so running it again never duplicates data.
CREATE OR REPLACE PROCEDURE raw.sp_load_raw()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    COPY INTO sentinal_motor.raw.policies_raw
        FROM @sentinal_motor.raw.s3_stage/policies/
        FILE_FORMAT = (FORMAT_NAME = 'sentinal_motor.raw.ff_csv')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        INCLUDE_METADATA = (
            file_name       = METADATA$FILENAME,
            file_row_number = METADATA$FILE_ROW_NUMBER,
            loaded_at       = METADATA$START_SCAN_TIME
        )
        ON_ERROR = 'CONTINUE';

    COPY INTO sentinal_motor.raw.claims_raw (payload, file_name, file_row_number, loaded_at)
        FROM (
            SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
            FROM @sentinal_motor.raw.s3_stage/claims/
        )
        FILE_FORMAT = (FORMAT_NAME = 'sentinal_motor.raw.ff_json')
        ON_ERROR = 'CONTINUE';

    COPY INTO sentinal_motor.raw.telematics_raw (payload, file_name, file_row_number, loaded_at)
        FROM (
            SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME
            FROM @sentinal_motor.raw.s3_stage/telematics/
        )
        FILE_FORMAT = (FORMAT_NAME = 'sentinal_motor.raw.ff_json')
        ON_ERROR = 'CONTINUE';

    RETURN 'raw load complete';
END;
$$;


-- =====================================================================
-- 5.5  STAGING: BATCH, CLEAN AND REJECTED TABLES + PROCEDURES
-- =====================================================================
-- Each entity has three tables:
--   *_batch     the rows taken from the raw stream in the CURRENT run,
--               typed, with a reject_reason (NULL = valid). Emptied each run.
--               (Replaces the earlier claims_flattened + claims_valid pair.)
--   *_rejected  quarantine: every failed row, with its reason. PII is
--               removed from the stored raw record.
--   *_clean     valid rows, one per key, kept up to date with MERGE.
-- Each procedure runs inside ONE transaction: if any statement fails,
-- everything rolls back and the raw stream keeps its rows for the next run.

-- ---------- Policies ----------
CREATE OR REPLACE TRANSIENT TABLE staging.policies_batch (
    policy_id          STRING,
    holder_name        STRING,
    ssn                STRING,
    state              STRING,
    vehicle_id         STRING,
    premium_amount     NUMBER(10,2),
    policy_start_date  DATE,
    policy_end_date    DATE,
    updated_at         TIMESTAMP_NTZ,
    file_name          STRING,
    file_row_number    NUMBER,
    loaded_at          TIMESTAMP_LTZ,
    raw_record         VARIANT,
    reject_reason      STRING
);

CREATE OR REPLACE TABLE staging.policies_clean (
    policy_id          STRING NOT NULL,
    holder_name        STRING,
    ssn                STRING,
    state              STRING,
    vehicle_id         STRING,
    premium_amount     NUMBER(10,2),
    policy_start_date  DATE,
    policy_end_date    DATE,
    updated_at         TIMESTAMP_NTZ,
    file_name          STRING,
    loaded_at          TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE staging.policies_rejected (
    policy_id        STRING,
    reject_reason    STRING,
    raw_record       VARIANT,        -- original values, without name and SSN
    file_name        STRING,
    file_row_number  NUMBER,
    loaded_at        TIMESTAMP_LTZ,
    rejected_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE staging.sp_stage_policies()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    n_read     INTEGER DEFAULT 0;
    n_rejected INTEGER DEFAULT 0;
    n_merged   INTEGER DEFAULT 0;
BEGIN
    BEGIN TRANSACTION;

    DELETE FROM sentinal_motor.staging.policies_batch;

    -- 1. Take new rows from the raw stream, convert types, decide validity
    INSERT INTO sentinal_motor.staging.policies_batch (
        policy_id, holder_name, ssn, state, vehicle_id, premium_amount,
        policy_start_date, policy_end_date, updated_at,
        file_name, file_row_number, loaded_at, raw_record, reject_reason)
    SELECT t.*,
           CASE
               WHEN t.policy_id IS NULL                      THEN 'missing_policy_id'
               WHEN t.state IS NULL                          THEN 'missing_state'
               WHEN t.premium_amount IS NULL                 THEN 'invalid_premium'
               WHEN t.premium_amount <= 0                    THEN 'non_positive_premium'
               WHEN t.policy_start_date IS NULL              THEN 'invalid_start_date'
               WHEN t.policy_end_date IS NULL
                 OR t.policy_end_date < t.policy_start_date  THEN 'invalid_end_date'
           END
    FROM (
        SELECT
            NULLIF(TRIM(policy_id), '')                    AS policy_id,
            holder_name,
            ssn,
            NULLIF(TRIM(state), '')                        AS state,
            vehicle_id,
            TRY_TO_NUMBER(premium_amount, 10, 2)           AS premium_amount,
            TRY_TO_DATE(policy_start_date, 'YYYY-MM-DD')   AS policy_start_date,
            TRY_TO_DATE(policy_end_date, 'YYYY-MM-DD')     AS policy_end_date,
            TRY_TO_TIMESTAMP_NTZ(updated_at)               AS updated_at,
            file_name,
            file_row_number,
            loaded_at,
            OBJECT_CONSTRUCT(                               -- no name, no SSN
                'policy_id', policy_id, 'state', state, 'vehicle_id', vehicle_id,
                'premium_amount', premium_amount,
                'policy_start_date', policy_start_date,
                'policy_end_date', policy_end_date, 'updated_at', updated_at
            )                                               AS raw_record
        FROM sentinal_motor.raw.policies_raw_stream
    ) t;
    n_read := SQLROWCOUNT;

    -- 2. Quarantine the failures
    INSERT INTO sentinal_motor.staging.policies_rejected
        (policy_id, reject_reason, raw_record, file_name, file_row_number, loaded_at)
    SELECT policy_id, reject_reason, raw_record, file_name, file_row_number, loaded_at
    FROM sentinal_motor.staging.policies_batch
    WHERE reject_reason IS NOT NULL;
    n_rejected := SQLROWCOUNT;

    -- 3. Upsert the valid rows: one per policy, newest version wins
    MERGE INTO sentinal_motor.staging.policies_clean t
    USING (
        SELECT *
        FROM sentinal_motor.staging.policies_batch
        WHERE reject_reason IS NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY policy_id
            ORDER BY updated_at DESC NULLS LAST, loaded_at DESC, file_row_number DESC
        ) = 1
    ) s
    ON t.policy_id = s.policy_id
    WHEN MATCHED AND (t.updated_at IS NULL OR s.updated_at > t.updated_at) THEN UPDATE SET
        holder_name = s.holder_name, ssn = s.ssn, state = s.state,
        vehicle_id = s.vehicle_id, premium_amount = s.premium_amount,
        policy_start_date = s.policy_start_date, policy_end_date = s.policy_end_date,
        updated_at = s.updated_at, file_name = s.file_name, loaded_at = s.loaded_at
    WHEN NOT MATCHED THEN INSERT (
        policy_id, holder_name, ssn, state, vehicle_id, premium_amount,
        policy_start_date, policy_end_date, updated_at, file_name, loaded_at)
    VALUES (
        s.policy_id, s.holder_name, s.ssn, s.state, s.vehicle_id, s.premium_amount,
        s.policy_start_date, s.policy_end_date, s.updated_at, s.file_name, s.loaded_at);
    n_merged := SQLROWCOUNT;

    COMMIT;
    RETURN 'policies: ' || n_read || ' read, ' || n_rejected || ' rejected, '
           || n_merged || ' inserted/updated';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
$$;

-- ---------- Claims ----------
CREATE OR REPLACE TRANSIENT TABLE staging.claims_batch (
    claim_id          STRING,
    policy_id         STRING,
    claimant_name     STRING,
    ssn               STRING,
    is_policy_holder  BOOLEAN,
    loss_date         DATE,
    loss_description  STRING,
    filed_date        DATE,
    settlement_date   DATE,
    claim_amount      NUMBER(12,2),
    claim_status      STRING,
    updated_at        TIMESTAMP_NTZ,
    file_name         STRING,
    file_row_number   NUMBER,
    loaded_at         TIMESTAMP_LTZ,
    raw_record        VARIANT,
    reject_reason     STRING
);

CREATE OR REPLACE TABLE staging.claims_cleaned (
    claim_id          STRING NOT NULL,
    policy_id         STRING,
    claimant_name     STRING,
    ssn               STRING,
    is_policy_holder  BOOLEAN,
    loss_date         DATE,
    loss_description  STRING,
    filed_date        DATE,
    settlement_date   DATE,
    claim_amount      NUMBER(12,2),
    claim_status      STRING,
    updated_at        TIMESTAMP_NTZ,
    file_name         STRING,
    loaded_at         TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE staging.claims_rejected (
    claim_id         STRING,
    policy_id        STRING,
    reject_reason    STRING,
    raw_record       VARIANT,        -- original JSON without the claimant block
    file_name        STRING,
    file_row_number  NUMBER,
    loaded_at        TIMESTAMP_LTZ,
    rejected_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE staging.sp_stage_claims()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    n_read     INTEGER DEFAULT 0;
    n_rejected INTEGER DEFAULT 0;
    n_merged   INTEGER DEFAULT 0;
BEGIN
    BEGIN TRANSACTION;

    DELETE FROM sentinal_motor.staging.claims_batch;

    -- 1. Flatten the JSON, convert types, check row rules AND that the policy exists
    INSERT INTO sentinal_motor.staging.claims_batch (
        claim_id, policy_id, claimant_name, ssn, is_policy_holder,
        loss_date, loss_description, filed_date, settlement_date,
        claim_amount, claim_status, updated_at,
        file_name, file_row_number, loaded_at, raw_record, reject_reason)
    SELECT t.*,
           CASE
               WHEN t.claim_id IS NULL                  THEN 'missing_claim_id'
               WHEN t.policy_id IS NULL                 THEN 'missing_policy_id'
               WHEN t.filed_date IS NULL                THEN 'invalid_filed_date'
               WHEN t.claim_amount IS NULL              THEN 'invalid_claim_amount'
               WHEN t.claim_amount < 0                  THEN 'negative_claim_amount'
               WHEN t.settlement_date < t.filed_date    THEN 'settlement_before_filed'
               WHEN p.policy_id IS NULL                 THEN 'policy_not_found'
           END
    FROM (
        SELECT
            NULLIF(TRIM(payload:claim_id::STRING), '')                AS claim_id,
            NULLIF(TRIM(payload:policy_id::STRING), '')               AS policy_id,
            payload:claimant.name::STRING                             AS claimant_name,
            payload:claimant.ssn::STRING                              AS ssn,
            TRY_TO_BOOLEAN(payload:claimant.is_policy_holder::STRING) AS is_policy_holder,
            TRY_TO_DATE(payload:loss.loss_date::STRING, 'YYYY-MM-DD') AS loss_date,
            payload:loss.description::STRING                          AS loss_description,
            TRY_TO_DATE(payload:filed_date::STRING, 'YYYY-MM-DD')     AS filed_date,
            TRY_TO_DATE(payload:settlement_date::STRING, 'YYYY-MM-DD') AS settlement_date,
            TRY_TO_NUMBER(payload:claim_amount::STRING, 12, 2)        AS claim_amount,
            UPPER(TRIM(payload:claim_status::STRING))                 AS claim_status,
            TRY_TO_TIMESTAMP_NTZ(payload:updated_at::STRING)          AS updated_at,
            file_name,
            file_row_number,
            loaded_at,
            OBJECT_DELETE(payload, 'claimant')                        AS raw_record
        FROM sentinal_motor.raw.claims_raw_stream
    ) t
    LEFT JOIN sentinal_motor.staging.policies_clean p
        ON t.policy_id = p.policy_id;
    n_read := SQLROWCOUNT;

    -- 2. Quarantine the failures
    INSERT INTO sentinal_motor.staging.claims_rejected
        (claim_id, policy_id, reject_reason, raw_record, file_name, file_row_number, loaded_at)
    SELECT claim_id, policy_id, reject_reason, raw_record, file_name, file_row_number, loaded_at
    FROM sentinal_motor.staging.claims_batch
    WHERE reject_reason IS NOT NULL;
    n_rejected := SQLROWCOUNT;

    -- 3. Upsert the valid claims: one per claim_id, newest version wins
    MERGE INTO sentinal_motor.staging.claims_cleaned t
    USING (
        SELECT *
        FROM sentinal_motor.staging.claims_batch
        WHERE reject_reason IS NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY claim_id
            ORDER BY updated_at DESC NULLS LAST, loaded_at DESC, file_row_number DESC
        ) = 1
    ) s
    ON t.claim_id = s.claim_id
    WHEN MATCHED AND (t.updated_at IS NULL OR s.updated_at > t.updated_at) THEN UPDATE SET
        policy_id = s.policy_id, claimant_name = s.claimant_name, ssn = s.ssn,
        is_policy_holder = s.is_policy_holder, loss_date = s.loss_date,
        loss_description = s.loss_description, filed_date = s.filed_date,
        settlement_date = s.settlement_date, claim_amount = s.claim_amount,
        claim_status = s.claim_status, updated_at = s.updated_at,
        file_name = s.file_name, loaded_at = s.loaded_at
    WHEN NOT MATCHED THEN INSERT (
        claim_id, policy_id, claimant_name, ssn, is_policy_holder, loss_date,
        loss_description, filed_date, settlement_date, claim_amount, claim_status,
        updated_at, file_name, loaded_at)
    VALUES (
        s.claim_id, s.policy_id, s.claimant_name, s.ssn, s.is_policy_holder, s.loss_date,
        s.loss_description, s.filed_date, s.settlement_date, s.claim_amount, s.claim_status,
        s.updated_at, s.file_name, s.loaded_at);
    n_merged := SQLROWCOUNT;

    COMMIT;
    RETURN 'claims: ' || n_read || ' read, ' || n_rejected || ' rejected, '
           || n_merged || ' inserted/updated';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
$$;

-- ---------- Telematics ----------
CREATE OR REPLACE TRANSIENT TABLE staging.telematics_batch (
    trip_id          STRING,
    policy_id        STRING,
    trip_date        DATE,
    trip_miles       NUMBER(8,1),
    risk_score       NUMBER(5,1),
    event            STRING,
    recorded_at      TIMESTAMP_NTZ,
    file_name        STRING,
    file_row_number  NUMBER,
    loaded_at        TIMESTAMP_LTZ,
    raw_record       VARIANT,
    reject_reason    STRING
);

CREATE OR REPLACE TABLE staging.telematics_cleaned (
    trip_id          STRING NOT NULL,
    policy_id        STRING,
    trip_date        DATE,
    trip_miles       NUMBER(8,1),
    risk_score       NUMBER(5,1),    -- NULL = not scored; kept, not rejected
    event            STRING,
    recorded_at      TIMESTAMP_NTZ,
    file_name        STRING,
    loaded_at        TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE staging.telematics_rejected (
    trip_id          STRING,
    policy_id        STRING,
    reject_reason    STRING,
    raw_record       VARIANT,
    file_name        STRING,
    file_row_number  NUMBER,
    loaded_at        TIMESTAMP_LTZ,
    rejected_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE staging.sp_stage_telematics()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    n_read     INTEGER DEFAULT 0;
    n_rejected INTEGER DEFAULT 0;
    n_merged   INTEGER DEFAULT 0;
BEGIN
    BEGIN TRANSACTION;

    DELETE FROM sentinal_motor.staging.telematics_batch;

    INSERT INTO sentinal_motor.staging.telematics_batch (
        trip_id, policy_id, trip_date, trip_miles, risk_score, event, recorded_at,
        file_name, file_row_number, loaded_at, raw_record, reject_reason)
    SELECT t.*,
           CASE
               WHEN t.trip_id IS NULL                       THEN 'missing_trip_id'
               WHEN t.policy_id IS NULL                     THEN 'missing_policy_id'
               WHEN t.trip_date IS NULL                     THEN 'invalid_trip_date'
               WHEN t.trip_miles IS NULL OR t.trip_miles < 0 THEN 'invalid_trip_miles'
               WHEN t.risk_score < 0 OR t.risk_score > 100  THEN 'risk_score_out_of_range'
               WHEN p.policy_id IS NULL                     THEN 'policy_not_found'
           END
    FROM (
        SELECT
            NULLIF(TRIM(payload:trip_id::STRING), '')               AS trip_id,
            NULLIF(TRIM(payload:policy_id::STRING), '')             AS policy_id,
            TRY_TO_DATE(payload:trip_date::STRING, 'YYYY-MM-DD')    AS trip_date,
            TRY_TO_NUMBER(payload:trip_miles::STRING, 8, 1)         AS trip_miles,
            TRY_TO_NUMBER(payload:risk_score::STRING, 5, 1)         AS risk_score,
            UPPER(TRIM(payload:event::STRING))                      AS event,
            TRY_TO_TIMESTAMP_NTZ(payload:recorded_at::STRING)       AS recorded_at,
            file_name,
            file_row_number,
            loaded_at,
            payload                                                 AS raw_record
        FROM sentinal_motor.raw.telematics_raw_stream
    ) t
    LEFT JOIN sentinal_motor.staging.policies_clean p
        ON t.policy_id = p.policy_id;
    n_read := SQLROWCOUNT;

    INSERT INTO sentinal_motor.staging.telematics_rejected
        (trip_id, policy_id, reject_reason, raw_record, file_name, file_row_number, loaded_at)
    SELECT trip_id, policy_id, reject_reason, raw_record, file_name, file_row_number, loaded_at
    FROM sentinal_motor.staging.telematics_batch
    WHERE reject_reason IS NOT NULL;
    n_rejected := SQLROWCOUNT;

    MERGE INTO sentinal_motor.staging.telematics_cleaned t
    USING (
        SELECT *
        FROM sentinal_motor.staging.telematics_batch
        WHERE reject_reason IS NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY recorded_at DESC NULLS LAST, loaded_at DESC, file_row_number DESC
        ) = 1
    ) s
    ON t.trip_id = s.trip_id
    WHEN MATCHED AND (t.recorded_at IS NULL OR s.recorded_at > t.recorded_at) THEN UPDATE SET
        policy_id = s.policy_id, trip_date = s.trip_date, trip_miles = s.trip_miles,
        risk_score = s.risk_score, event = s.event, recorded_at = s.recorded_at,
        file_name = s.file_name, loaded_at = s.loaded_at
    WHEN NOT MATCHED THEN INSERT (
        trip_id, policy_id, trip_date, trip_miles, risk_score, event, recorded_at,
        file_name, loaded_at)
    VALUES (
        s.trip_id, s.policy_id, s.trip_date, s.trip_miles, s.risk_score, s.event,
        s.recorded_at, s.file_name, s.loaded_at);
    n_merged := SQLROWCOUNT;

    COMMIT;
    RETURN 'telematics: ' || n_read || ' read, ' || n_rejected || ' rejected, '
           || n_merged || ' inserted/updated';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
$$;


-- =====================================================================
-- 5.6  CURATED: TABLES, STREAMS ON STAGING, REFRESH PROCEDURES, VIEWS
-- =====================================================================
-- Curated tables are created ONCE here and from then on refreshed with
-- MERGE. That keeps their tags and masking in place (CREATE OR REPLACE
-- would drop them on every run).

CREATE OR REPLACE TABLE curated.dim_policy (
    policy_id          STRING NOT NULL,
    holder_name        STRING,
    ssn                STRING,
    state              STRING,
    vehicle_id         STRING,
    premium_amount     NUMBER(10,2),
    policy_start_date  DATE,
    policy_end_date    DATE,
    source_updated_at  TIMESTAMP_NTZ,
    curated_at         TIMESTAMP_LTZ     -- when this row last changed in curated
);

CREATE OR REPLACE TABLE curated.fact_claims (
    claim_id           STRING NOT NULL,
    policy_id          STRING,
    claimant_name      STRING,
    ssn                STRING,
    is_policy_holder   BOOLEAN,
    loss_date          DATE,
    loss_description   STRING,
    filed_date         DATE,
    settlement_date    DATE,
    claim_amount       NUMBER(12,2),
    claim_status       STRING,
    days_to_settle     NUMBER,
    is_high_value      BOOLEAN,
    source_updated_at  TIMESTAMP_NTZ,
    curated_at         TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE curated.fact_driver_risk (
    policy_id          STRING NOT NULL,
    total_trips        NUMBER,
    total_miles        NUMBER(12,1),
    total_hard_brakes  NUMBER,
    avg_risk_score     NUMBER(5,2),
    trips_scored       NUMBER,
    pct_trips_scored   NUMBER(5,4),
    is_unscored        BOOLEAN,
    curated_at         TIMESTAMP_LTZ
);

-- Standard (not append-only) streams, because staging is MERGEd: they
-- capture inserts AND updates. An update appears as a DELETE + INSERT pair;
-- reading only METADATA$ACTION = 'INSERT' gives the new version of each row.
CREATE OR REPLACE STREAM staging.policies_clean_stream     ON TABLE staging.policies_clean;
CREATE OR REPLACE STREAM staging.claims_cleaned_stream     ON TABLE staging.claims_cleaned;
CREATE OR REPLACE STREAM staging.telematics_cleaned_stream ON TABLE staging.telematics_cleaned;

CREATE OR REPLACE PROCEDURE curated.sp_refresh_dim_policy()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO sentinal_motor.curated.dim_policy t
    USING (
        SELECT policy_id, holder_name, ssn, state, vehicle_id, premium_amount,
               policy_start_date, policy_end_date, updated_at
        FROM sentinal_motor.staging.policies_clean_stream
        WHERE METADATA$ACTION = 'INSERT'
    ) s
    ON t.policy_id = s.policy_id
    WHEN MATCHED THEN UPDATE SET
        holder_name = s.holder_name, ssn = s.ssn, state = s.state,
        vehicle_id = s.vehicle_id, premium_amount = s.premium_amount,
        policy_start_date = s.policy_start_date, policy_end_date = s.policy_end_date,
        source_updated_at = s.updated_at, curated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        policy_id, holder_name, ssn, state, vehicle_id, premium_amount,
        policy_start_date, policy_end_date, source_updated_at, curated_at)
    VALUES (
        s.policy_id, s.holder_name, s.ssn, s.state, s.vehicle_id, s.premium_amount,
        s.policy_start_date, s.policy_end_date, s.updated_at, CURRENT_TIMESTAMP());
    RETURN 'dim_policy: ' || SQLROWCOUNT || ' rows inserted/updated';
END;
$$;

CREATE OR REPLACE PROCEDURE curated.sp_refresh_fact_claims()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO sentinal_motor.curated.fact_claims t
    USING (
        SELECT claim_id, policy_id, claimant_name, ssn, is_policy_holder, loss_date,
               loss_description, filed_date, settlement_date, claim_amount, claim_status,
               DATEDIFF('day', filed_date, settlement_date)   AS days_to_settle,
               COALESCE(claim_amount > 20000, FALSE)          AS is_high_value, -- threshold: agree with the business
               updated_at
        FROM sentinal_motor.staging.claims_cleaned_stream
        WHERE METADATA$ACTION = 'INSERT'
    ) s
    ON t.claim_id = s.claim_id
    WHEN MATCHED THEN UPDATE SET
        policy_id = s.policy_id, claimant_name = s.claimant_name, ssn = s.ssn,
        is_policy_holder = s.is_policy_holder, loss_date = s.loss_date,
        loss_description = s.loss_description, filed_date = s.filed_date,
        settlement_date = s.settlement_date, claim_amount = s.claim_amount,
        claim_status = s.claim_status, days_to_settle = s.days_to_settle,
        is_high_value = s.is_high_value, source_updated_at = s.updated_at,
        curated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        claim_id, policy_id, claimant_name, ssn, is_policy_holder, loss_date,
        loss_description, filed_date, settlement_date, claim_amount, claim_status,
        days_to_settle, is_high_value, source_updated_at, curated_at)
    VALUES (
        s.claim_id, s.policy_id, s.claimant_name, s.ssn, s.is_policy_holder, s.loss_date,
        s.loss_description, s.filed_date, s.settlement_date, s.claim_amount, s.claim_status,
        s.days_to_settle, s.is_high_value, s.updated_at, CURRENT_TIMESTAMP());
    RETURN 'fact_claims: ' || SQLROWCOUNT || ' rows inserted/updated';
END;
$$;

-- Driver risk is an aggregate, so a new trip changes its policy's totals.
-- Incremental pattern: find the policies touched by new trips, recompute
-- only those policies from the full clean table, and MERGE the results.
CREATE OR REPLACE PROCEDURE curated.sp_refresh_fact_driver_risk()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO sentinal_motor.curated.fact_driver_risk t
    USING (
        SELECT
            c.policy_id,
            COUNT(*)                                   AS total_trips,
            SUM(c.trip_miles)                          AS total_miles,
            COUNT_IF(c.event = 'HARD_BRAKE')           AS total_hard_brakes,
            ROUND(AVG(c.risk_score), 2)                AS avg_risk_score,   -- NULL scores excluded
            COUNT(c.risk_score)                        AS trips_scored,
            ROUND(COUNT(c.risk_score) / COUNT(*), 4)   AS pct_trips_scored,
            COUNT(c.risk_score) = 0                    AS is_unscored
        FROM sentinal_motor.staging.telematics_cleaned c
        WHERE c.policy_id IN (
            SELECT DISTINCT policy_id
            FROM sentinal_motor.staging.telematics_cleaned_stream
            WHERE METADATA$ACTION = 'INSERT'
        )
        GROUP BY c.policy_id
    ) s
    ON t.policy_id = s.policy_id
    WHEN MATCHED THEN UPDATE SET
        total_trips = s.total_trips, total_miles = s.total_miles,
        total_hard_brakes = s.total_hard_brakes, avg_risk_score = s.avg_risk_score,
        trips_scored = s.trips_scored, pct_trips_scored = s.pct_trips_scored,
        is_unscored = s.is_unscored, curated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        policy_id, total_trips, total_miles, total_hard_brakes, avg_risk_score,
        trips_scored, pct_trips_scored, is_unscored, curated_at)
    VALUES (
        s.policy_id, s.total_trips, s.total_miles, s.total_hard_brakes, s.avg_risk_score,
        s.trips_scored, s.pct_trips_scored, s.is_unscored, CURRENT_TIMESTAMP());
    RETURN 'fact_driver_risk: ' || SQLROWCOUNT || ' policies inserted/updated';
END;
$$;

-- claim_age_days changes every day, so it lives in a view, not the table.
CREATE OR REPLACE VIEW curated.vw_fact_claims AS
SELECT f.*,
       CASE WHEN f.settlement_date IS NULL
            THEN DATEDIFF('day', f.filed_date, CURRENT_DATE())
       END AS claim_age_days
FROM curated.fact_claims f;

CREATE OR REPLACE VIEW curated.vw_loss_ratio_by_state AS
WITH claims_by_policy AS (
    SELECT policy_id, COUNT(*) AS claim_count, SUM(claim_amount) AS claims_amount
    FROM curated.fact_claims
    GROUP BY policy_id
)
SELECT
    p.state,
    COUNT(*)                                       AS policies,
    COALESCE(SUM(c.claim_count), 0)                AS claims,
    SUM(p.premium_amount)                          AS premiums_collected,
    COALESCE(SUM(c.claims_amount), 0)              AS claims_amount,
    ROUND(COALESCE(SUM(c.claims_amount), 0)
          / NULLIF(SUM(p.premium_amount), 0), 4)   AS loss_ratio
FROM curated.dim_policy p
LEFT JOIN claims_by_policy c ON p.policy_id = c.policy_id
GROUP BY p.state;

CREATE OR REPLACE VIEW curated.vw_risk_vs_claims AS
WITH claims_by_policy AS (
    SELECT policy_id, COUNT(*) AS claim_count
    FROM curated.fact_claims
    GROUP BY policy_id
),
base AS (
    SELECT r.policy_id, r.avg_risk_score, r.total_miles,
           COALESCE(c.claim_count, 0) AS claim_count
    FROM curated.fact_driver_risk r
    LEFT JOIN claims_by_policy c ON r.policy_id = c.policy_id
    WHERE r.is_unscored = FALSE
),
banded AS (
    SELECT *, NTILE(4) OVER (ORDER BY avg_risk_score) AS risk_quartile
    FROM base
)
SELECT
    risk_quartile,
    MIN(avg_risk_score)                              AS min_score,
    MAX(avg_risk_score)                              AS max_score,
    COUNT(*)                                         AS policies,
    SUM(claim_count)                                 AS claims,
    ROUND(SUM(claim_count) / COUNT(*), 4)            AS claims_per_policy,
    ROUND(COUNT_IF(claim_count > 0) / COUNT(*), 4)   AS pct_policies_with_claim,
    ROUND(SUM(claim_count) / NULLIF(SUM(total_miles), 0) * 1000, 4) AS claims_per_1000_miles
FROM banded
GROUP BY risk_quartile;

-- Re-apply the PII tags: the tables above are new, so they have no tags yet.
-- Rejected tables are not tagged because the procedures strip PII out.
-- raw.claims_raw / raw.telematics_raw hold PII inside VARIANT JSON, which a
-- STRING masking policy cannot cover: keep the RAW schema to the pipeline
-- role and SYSADMIN only (never grant it to analysts or engineers).
ALTER TABLE raw.policies_raw         MODIFY COLUMN holder_name   SET TAG governance.pii_type = 'name';
ALTER TABLE raw.policies_raw         MODIFY COLUMN ssn           SET TAG governance.pii_type = 'ssn';
ALTER TABLE staging.policies_batch   MODIFY COLUMN holder_name   SET TAG governance.pii_type = 'name';
ALTER TABLE staging.policies_batch   MODIFY COLUMN ssn           SET TAG governance.pii_type = 'ssn';
ALTER TABLE staging.policies_clean   MODIFY COLUMN holder_name   SET TAG governance.pii_type = 'name';
ALTER TABLE staging.policies_clean   MODIFY COLUMN ssn           SET TAG governance.pii_type = 'ssn';
ALTER TABLE staging.claims_batch     MODIFY COLUMN claimant_name SET TAG governance.pii_type = 'name';
ALTER TABLE staging.claims_batch     MODIFY COLUMN ssn           SET TAG governance.pii_type = 'ssn';
ALTER TABLE staging.claims_cleaned   MODIFY COLUMN claimant_name SET TAG governance.pii_type = 'name';
ALTER TABLE staging.claims_cleaned   MODIFY COLUMN ssn           SET TAG governance.pii_type = 'ssn';
ALTER TABLE curated.dim_policy       MODIFY COLUMN holder_name   SET TAG governance.pii_type = 'name';
ALTER TABLE curated.dim_policy       MODIFY COLUMN ssn           SET TAG governance.pii_type = 'ssn';
ALTER TABLE curated.fact_claims      MODIFY COLUMN claimant_name SET TAG governance.pii_type = 'name';
ALTER TABLE curated.fact_claims      MODIFY COLUMN ssn           SET TAG governance.pii_type = 'ssn';
