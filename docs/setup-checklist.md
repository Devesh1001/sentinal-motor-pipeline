# Setup checklist: what to make sure of

Checks to make before each stage, so the problems in [lessons-learned.md](lessons-learned.md) don't happen. The number in brackets points to the related lesson.

## Before you start

- [ ] Snowflake account is **Enterprise edition or a trial**: masking policies and tags are not available on Standard.
- [ ] Decide which role owns objects (**SYSADMIN**) and create everything with it. Use ACCOUNTADMIN only for the storage integration and account-level grants. (6)
- [ ] Know your warehouse name; the scripts use `compute_wh`.
- [ ] Set a **budget alert** in AWS and a **resource monitor** or auto-suspend (60 seconds) on the Snowflake warehouse.

## AWS

- [ ] S3 bucket in the **same region** as the Snowflake account (`SELECT CURRENT_REGION();`). Newer regions are opt-in and must be enabled in the AWS account first.
- [ ] Block public access left on.
- [ ] Generator user and Snowflake role limited to the `sentinal_motor/` prefix only.
- [ ] Trust policy uses the **exact** IAM user ARN and external ID from `DESC INTEGRATION`, and the integration is never recreated afterwards.
- [ ] `LIST @sentinal_motor.raw.s3_stage;` shows the files before going further.

## Snowflake objects

- [ ] File formats have the right options; `IF NOT EXISTS` keeps old settings without warning, so check with `DESC FILE FORMAT`: (7)
  - CSV: `PARSE_HEADER = TRUE`, `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE`, `SKIP_HEADER = 0`
  - JSON: `STRIP_OUTER_ARRAY = TRUE`
- [ ] Streams are created **before** the data they should capture, or with `SHOW_INITIAL_ROWS = TRUE`. (8)
- [ ] After every load, check `COPY_HISTORY`: `ON_ERROR = 'CONTINUE'` can load 0 rows without failing. (7)
- [ ] Curated tables are created once and refreshed with `MERGE`, never `CREATE OR REPLACE`, so tags and masking stay attached. (3)
- [ ] Policies are staged before claims and telematics, which are checked against them. (1)

## Security

- [ ] The pipeline role exists and has its masking branch **before** the first run; otherwise masked text gets stored as data. (2)
- [ ] Every PII column is tagged in every layer, including batch tables; check with `policy_references`.
- [ ] Test each role with `USE SECONDARY ROLES NONE`. (5)
- [ ] Change an attached masking policy with `ALTER MASKING POLICY ... SET BODY`. (4)
- [ ] The RAW schema (PII inside JSON) is granted only to SYSADMIN and the pipeline role.
- [ ] Temporary testing grants (`sm_pipeline` to your own user) are revoked afterwards.

## Airflow

- [ ] Docker Desktop is running before `astro dev start`; the project is not in a OneDrive-synced folder or a path with spaces.
- [ ] The private key **file** is copied (not pasted text) into `include/keys/`, then `astro dev restart`. (9)
- [ ] The connection has `"authenticator": "SNOWFLAKE_JWT"` and an empty password. (9)
- [ ] The first run after a manual load is expected to stop at `new_data_to_process` with the rest skipped.
- [ ] `max_active_runs = 1`, so two runs never consume the same streams.

## Before publishing

- [ ] `git status` lists no keys, `airflow_settings.yaml`, `generator_state.json`, `output/` or logs.
- [ ] `git grep -n "BEGIN PRIVATE KEY"` and `git grep -n "AKIA"` return nothing.
- [ ] Screenshots show no account identifiers, usernames, AWS account numbers, or unmasked names and SSNs.
- [ ] Image files are in `docs/images/` with lowercase names that match the README exactly.
