-- =====================================================================
-- SENTINAL MOTOR - LAYER 5 VERIFICATION
-- Run as SYSADMIN (counts only; names and SSNs show masked, as they should).
-- =====================================================================
USE ROLE SYSADMIN;
USE SECONDARY ROLES NONE;
USE WAREHOUSE compute_wh;
USE DATABASE sentinal_motor;


-- =====================================================================
-- A. AFTER THE FIRST MANUAL RUN (end of 5.7)
-- =====================================================================

-- A1. Rows at each layer
SELECT 'policies'   AS entity,
       (SELECT COUNT(*) FROM raw.policies_raw)            AS raw_rows,
       (SELECT COUNT(*) FROM staging.policies_rejected)   AS rejected,
       (SELECT COUNT(*) FROM staging.policies_clean)      AS clean,
       (SELECT COUNT(*) FROM curated.dim_policy)          AS curated
UNION ALL
SELECT 'claims',
       (SELECT COUNT(*) FROM raw.claims_raw),
       (SELECT COUNT(*) FROM staging.claims_rejected),
       (SELECT COUNT(*) FROM staging.claims_cleaned),
       (SELECT COUNT(*) FROM curated.fact_claims)
UNION ALL
SELECT 'telematics',
       (SELECT COUNT(*) FROM raw.telematics_raw),
       (SELECT COUNT(*) FROM staging.telematics_rejected),
       (SELECT COUNT(*) FROM staging.telematics_cleaned),
       (SELECT SUM(total_trips) FROM curated.fact_driver_risk);
-- Expect: clean = curated on every row. raw - rejected - clean = duplicates removed.

-- A2. Why rows were rejected
SELECT 'policies' AS entity, reject_reason, COUNT(*) AS n FROM staging.policies_rejected GROUP BY 2
UNION ALL
SELECT 'claims',     reject_reason, COUNT(*) FROM staging.claims_rejected     GROUP BY 2
UNION ALL
SELECT 'telematics', reject_reason, COUNT(*) FROM staging.telematics_rejected GROUP BY 2
ORDER BY entity, n DESC;
-- Claims whose policy was itself rejected (e.g. premium 'N/A') show as
-- policy_not_found. That is correct: the policy never became valid.

-- A3. Every stream drained (all six should be FALSE)
SELECT SYSTEM$STREAM_HAS_DATA('SENTINAL_MOTOR.RAW.POLICIES_RAW_STREAM')            AS raw_policies,
       SYSTEM$STREAM_HAS_DATA('SENTINAL_MOTOR.RAW.CLAIMS_RAW_STREAM')              AS raw_claims,
       SYSTEM$STREAM_HAS_DATA('SENTINAL_MOTOR.RAW.TELEMATICS_RAW_STREAM')          AS raw_trips,
       SYSTEM$STREAM_HAS_DATA('SENTINAL_MOTOR.STAGING.POLICIES_CLEAN_STREAM')      AS stg_policies,
       SYSTEM$STREAM_HAS_DATA('SENTINAL_MOTOR.STAGING.CLAIMS_CLEANED_STREAM')      AS stg_claims,
       SYSTEM$STREAM_HAS_DATA('SENTINAL_MOTOR.STAGING.TELEMATICS_CLEANED_STREAM')  AS stg_trips;

-- A4. Masking still works on the new tables
USE ROLE sm_analyst;
USE SECONDARY ROLES NONE;
SELECT holder_name, ssn, state, premium_amount FROM sentinal_motor.curated.dim_policy LIMIT 5;
-- expect *** MASKED *** and ***-**-****, real state and premium

USE ROLE sm_fraud_analyst;
USE SECONDARY ROLES NONE;
SELECT holder_name, ssn FROM sentinal_motor.curated.dim_policy LIMIT 5;
-- expect real names and XXX-XX-1234

USE ROLE sm_pipeline;
USE SECONDARY ROLES NONE;
SELECT holder_name, ssn FROM sentinal_motor.curated.dim_policy LIMIT 5;
-- expect real names and full SSNs (900-xx-xxxx fakes)

USE ROLE SYSADMIN;

-- A5. The reporting views still work
SELECT * FROM curated.vw_loss_ratio_by_state ORDER BY loss_ratio DESC;
SELECT * FROM curated.vw_risk_vs_claims ORDER BY risk_quartile;


-- =====================================================================
-- B. STEP 5.9: INSERT 5 NEW CLAIMS, RUN THE DAG, CONFIRM ONLY THOSE 5 MOVED
-- =====================================================================

-- B1. BEFORE generating the files: record the baseline. Copy the
--     test_started_at value; the queries below use it.
SELECT CURRENT_TIMESTAMP()                         AS test_started_at,
       (SELECT COUNT(*) FROM curated.fact_claims)  AS fact_claims_before,
       (SELECT COUNT(*) FROM curated.dim_policy)   AS dim_policy_before;

-- B2. Now, on your laptop:
--       python generate_data.py incremental --claims 5 --clean --bucket <bucket> --profile sentinal
--     then trigger the DAG in the Airflow UI and wait until every task is green.
SET test_started_at = '<paste test_started_at here>';

-- B3. COPY loaded only the one new file (older files were skipped)
SELECT file_name, row_count, status, last_load_time
FROM TABLE(sentinal_motor.information_schema.copy_history(
    table_name => 'SENTINAL_MOTOR.RAW.CLAIMS_RAW',
    start_time => $test_started_at::TIMESTAMP_LTZ))
ORDER BY last_load_time DESC;
-- expect exactly 1 row: the new claims file, row_count = 5, status = Loaded

-- B4. Exactly 5 claims changed in curated, and they are the new ones
SELECT claim_id, policy_id, claim_amount, claim_status, days_to_settle, curated_at
FROM curated.fact_claims
WHERE curated_at >= $test_started_at::TIMESTAMP_LTZ
ORDER BY claim_id;
-- expect 5 rows with the highest claim_ids

SELECT COUNT(*) AS fact_claims_after FROM curated.fact_claims;
-- expect fact_claims_before + 5

-- B5. Nothing else was reprocessed
SELECT
  (SELECT COUNT(*) FROM curated.dim_policy       WHERE curated_at >= $test_started_at::TIMESTAMP_LTZ) AS policies_touched,
  (SELECT COUNT(*) FROM curated.fact_driver_risk WHERE curated_at >= $test_started_at::TIMESTAMP_LTZ) AS risk_rows_touched,
  (SELECT COUNT(*) FROM staging.claims_rejected  WHERE rejected_at >= $test_started_at::TIMESTAMP_LTZ) AS new_rejects;
-- expect 0, 0, 0 (the files were --clean and contained no policies or trips)

-- B6. Streams drained again: rerun query A3, all FALSE.

-- B7. Idempotency: trigger the DAG again WITHOUT new files.
--     In the Airflow UI, new_data_to_process returns False and every task
--     after it shows as skipped (pink). Rerun B4: still 5 rows.


-- =====================================================================
-- C. OPTIONAL: PROVE THE QUARANTINE WORKS INCREMENTALLY
-- =====================================================================
-- On your laptop (every claim made invalid on purpose, plus some duplicates):
--   python generate_data.py incremental --claims 10 --dirty-rate 1.0 --bucket <bucket> --profile sentinal
-- Trigger the DAG, then:
SELECT reject_reason, COUNT(*) AS n
FROM staging.claims_rejected
WHERE rejected_at >= DATEADD('minute', -30, CURRENT_TIMESTAMP())
GROUP BY reject_reason
ORDER BY n DESC;
-- expect around 15 rows (10 bad claims + their duplicates), and
-- fact_claims unchanged.
