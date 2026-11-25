# Design decisions

| Decision | Alternative considered | Why |
|---|---|---|
| **Raw CSV columns are all `STRING`** | Typed raw columns | A value like `N/A` in a numeric column would make the row fail to load and disappear. As a string it lands, and staging records why it was rejected. |
| **Staging validates against staging** (`policies_clean`), not curated | Check claims against `dim_policy` | Keeps each layer depending only on the layer before it. Curated becomes a pure publish step. |
| **Quarantine instead of delete** | Drop invalid rows | Rejected rows keep a reason code, so failures can be counted by cause, investigated and reprocessed. PII is removed from the stored record. |
| **One transaction per staging procedure** | Separate autocommitted statements | Reading a stream advances it. If a later statement failed, the rows would be consumed but never processed. In one transaction, a failure rolls everything back and the stream keeps its rows. |
| **`MERGE` into curated, never `CREATE OR REPLACE`** | Rebuild tables each run | A rebuild reprocesses everything and silently drops column tags, which turns off masking. MERGE touches only changed rows and keeps tags. |
| **Missing risk scores are excluded from averages** | Default them to 0 | A missing score means unknown, not zero risk. Defaulting to 0 makes drivers look safer, which is the wrong direction for underwriting. `trips_scored` and `pct_trips_scored` show how reliable each average is. |
| **Driver risk refreshed per affected policy** | Recompute every policy | New trips change only their policy's totals. Recomputing just those policies keeps the run incremental even for an aggregate table. |
| **`claim_age_days` in a view** | Stored column | It changes every day. Stored, it would be wrong the day after it was written and would only update when the claim itself changed. |
| **Loss ratio aggregates claims per policy before joining** | Join policies to claims and sum | A direct join repeats a policy's premium once per claim and understates the loss ratio. |
| **Pipeline role has its own masking branch** | Run the pipeline as `sm_pii_reader` | Masking applies to the pipeline's own reads. A masked read would be written into the next table as data, and even PII readers only see `XXX-XX-1234`. A data quality check confirms no masked text is ever stored. |
| **Tag-based masking** | A policy attached to each column | One policy covers every tagged column in every layer. A new PII column only needs a tag. |
| **Service user with key-pair auth** (`TYPE = SERVICE`) | A normal user with a password | A service user cannot log in with a password at all, and the key never leaves Airflow. |
| **Storage integration with an IAM role** | AWS access keys in the stage | No long-lived AWS credentials in Snowflake. The external ID stops other Snowflake accounts from using the role. |
| **Airflow as the only scheduler** | Snowflake Tasks plus Airflow | Two schedulers consuming the same streams can race each other. `max_active_runs=1` prevents overlapping runs. |
| **Skip the run when streams are empty** | S3 key sensor | Streams already know whether there is new work, including work left by a failed earlier run, so one check covers both cases. |
