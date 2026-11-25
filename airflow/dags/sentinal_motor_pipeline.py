"""
Sentinal Motor pipeline: S3 -> raw -> staging -> curated, incremental via Snowflake streams.

Flow
    load_raw_from_s3          COPY INTO the three raw tables (already-loaded files are skipped)
    new_data_to_process       stop here if no stream holds new rows (nothing to do)
    stage_policies            policies first: claims and trips are checked against them
    stage_claims / stage_telematics
    refresh_dim_policy / refresh_fact_claims / refresh_fact_driver_risk
    check_*                   data quality gates: the run fails loudly if any check fails

All the SQL lives in Snowflake stored procedures (layer5_snowflake_setup.sql), so this
file only decides the order. Every procedure is safe to retry: the staging ones run in a
single transaction, and a failed run leaves the stream's rows for the next run.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum

try:  # Airflow 3
    from airflow.sdk import DAG, task
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.decorators import task

from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

CONN_ID = "snowflake_sentinal"
DB = "SENTINAL_MOTOR"

# Every stream the pipeline consumes. Staging streams are included so a run
# that failed half-way (raw done, curated not) is finished by the next run.
STREAMS = [
    "RAW.POLICIES_RAW_STREAM",
    "RAW.CLAIMS_RAW_STREAM",
    "RAW.TELEMATICS_RAW_STREAM",
    "STAGING.POLICIES_CLEAN_STREAM",
    "STAGING.CLAIMS_CLEANED_STREAM",
    "STAGING.TELEMATICS_CLEANED_STREAM",
]


def call_procedure(task_id: str, procedure: str) -> SQLExecuteQueryOperator:
    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id=CONN_ID,
        sql=f"CALL {DB}.{procedure}()",
        autocommit=True,
    )


with DAG(
    dag_id="sentinal_motor_pipeline",
    description="S3 to Snowflake curated layer, incremental with streams",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Kolkata"),
    schedule="@daily",          # switch to "@hourly" once everything works
    catchup=False,
    max_active_runs=1,          # two runs must never consume the same streams at once
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["sentinal_motor", "snowflake"],
    doc_md=__doc__,
) as dag:

    load_raw = call_procedure("load_raw_from_s3", "RAW.SP_LOAD_RAW")

    @task.short_circuit
    def new_data_to_process() -> bool:
        """True if any stream has rows waiting; False skips the rest of the run."""
        hook = SnowflakeHook(snowflake_conn_id=CONN_ID)
        sql = "SELECT " + ",\n       ".join(
            f"SYSTEM$STREAM_HAS_DATA('{DB}.{s}') AS \"{s}\"" for s in STREAMS
        )
        row = hook.get_first(sql)
        waiting = [name for name, has in zip(STREAMS, row) if has]
        print(f"Streams with new rows: {waiting or 'none'}")
        return bool(waiting)

    has_data = new_data_to_process()

    stage_policies = call_procedure("stage_policies", "STAGING.SP_STAGE_POLICIES")
    stage_claims = call_procedure("stage_claims", "STAGING.SP_STAGE_CLAIMS")
    stage_telematics = call_procedure("stage_telematics", "STAGING.SP_STAGE_TELEMATICS")

    refresh_dim_policy = call_procedure("refresh_dim_policy", "CURATED.SP_REFRESH_DIM_POLICY")
    refresh_fact_claims = call_procedure("refresh_fact_claims", "CURATED.SP_REFRESH_FACT_CLAIMS")
    refresh_driver_risk = call_procedure("refresh_fact_driver_risk", "CURATED.SP_REFRESH_FACT_DRIVER_RISK")

    # SQLCheckOperator fails the task if any value in the first row is false or 0.
    check_unique_keys = SQLCheckOperator(
        task_id="check_unique_keys",
        conn_id=CONN_ID,
        sql=f"""
        SELECT
          (SELECT COUNT(*) FROM (SELECT policy_id FROM {DB}.CURATED.DIM_POLICY
                                 GROUP BY policy_id HAVING COUNT(*) > 1)) = 0 AS policies_unique,
          (SELECT COUNT(*) FROM (SELECT claim_id FROM {DB}.CURATED.FACT_CLAIMS
                                 GROUP BY claim_id HAVING COUNT(*) > 1)) = 0 AS claims_unique,
          (SELECT COUNT(*) FROM (SELECT policy_id FROM {DB}.CURATED.FACT_DRIVER_RISK
                                 GROUP BY policy_id HAVING COUNT(*) > 1)) = 0 AS risk_unique
        """,
    )

    check_no_orphans = SQLCheckOperator(
        task_id="check_no_orphans",
        conn_id=CONN_ID,
        sql=f"""
        SELECT
          (SELECT COUNT(*) FROM {DB}.CURATED.FACT_CLAIMS f
             LEFT JOIN {DB}.CURATED.DIM_POLICY p ON f.policy_id = p.policy_id
            WHERE p.policy_id IS NULL) = 0 AS claims_have_policy,
          (SELECT COUNT(*) FROM {DB}.CURATED.FACT_DRIVER_RISK r
             LEFT JOIN {DB}.CURATED.DIM_POLICY p ON r.policy_id = p.policy_id
            WHERE p.policy_id IS NULL) = 0 AS risk_has_policy
        """,
    )

    # Catches the masking trap: masked text copied into a table as if it were data.
    check_no_masked_text_stored = SQLCheckOperator(
        task_id="check_no_masked_text_stored",
        conn_id=CONN_ID,
        sql=f"""
        SELECT
          (SELECT COUNT_IF(ssn LIKE '%*%' OR ssn LIKE 'XXX-%' OR holder_name LIKE '%MASKED%')
             FROM {DB}.CURATED.DIM_POLICY) = 0 AS dim_policy_clean,
          (SELECT COUNT_IF(ssn LIKE '%*%' OR ssn LIKE 'XXX-%' OR claimant_name LIKE '%MASKED%')
             FROM {DB}.CURATED.FACT_CLAIMS) = 0 AS fact_claims_clean
        """,
    )

    checks = [check_unique_keys, check_no_orphans, check_no_masked_text_stored]

    load_raw >> has_data >> stage_policies >> [stage_claims, stage_telematics]
    stage_policies >> refresh_dim_policy
    stage_claims >> refresh_fact_claims
    stage_telematics >> refresh_driver_risk
    for check in checks:
        [refresh_dim_policy, refresh_fact_claims, refresh_driver_risk] >> check
