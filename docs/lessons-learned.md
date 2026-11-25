# Lessons learned

Problems hit while building this pipeline, how each was traced, and what it changed.

## 1. 48 claims with no matching policy

**Symptom:** after building `fact_claims`, an orphan check found 48 claims whose `policy_id` was not in `dim_policy`.
**Cause:** the staging validation checked only row-level rules. Whether a policy exists needs a second table, so those claims passed.
**Fix:** a referential integrity check in staging against `policies_clean`, with failures quarantined as `policy_not_found`.
**Takeaway:** row-level checks and cross-table checks are different. Do both before data reaches curated, and keep an orphan check as a safety net.

## 2. Masked values would have been copied into tables as data

**Symptom:** found while designing the automated pipeline. Masking applies to every read, including the pipeline's.
**Cause:** a role without PII access reading a tagged column gets `*** MASKED ***`, and a MERGE would store that text as if it were the real name. Even the PII reader role only sees `XXX-XX-1234`.
**Fix:** a dedicated `sm_pipeline` role with its own branch in the masking policy, plus a DAG check that fails if masked text appears in a curated table.
**Takeaway:** security controls apply to the pipeline itself, not only to people.

## 3. Masking disappeared after a rebuild

**Symptom:** the tag check on `fact_claims` returned nothing after the table was rebuilt.
**Cause:** `CREATE OR REPLACE TABLE` creates a new table, without the old one's tags, so masking silently stopped.
**Fix:** curated tables are created once and refreshed with `MERGE`.

## 4. A masking policy could not be replaced

**Symptom:** `Policy MASK_PII_STRING cannot be dropped/replaced as it is associated with one or more entities.`
**Cause:** the policy was attached to a tag.
**Fix:** `ALTER MASKING POLICY ... SET BODY`, which changes the logic in place without a gap in protection.

## 5. Masking tests looked wrong because of secondary roles

**Symptom:** testing as the analyst role still showed real names.
**Cause:** secondary roles were active, so the session still held the fraud analyst role.
**Fix:** `USE SECONDARY ROLES NONE` before every role test, and checking `IS_ROLE_IN_SESSION` directly.

## 6. Objects owned by the wrong role

**Symptom:** `Insufficient privileges` or `does not exist or not authorized` when SYSADMIN tried to create schemas, policies or alter a file format.
**Cause:** objects created earlier under ACCOUNTADMIN.
**Fix:** `GRANT OWNERSHIP ... TO ROLE SYSADMIN COPY CURRENT GRANTS`.
**Takeaway:** decide which role owns objects before creating any, and keep ACCOUNTADMIN for account-level setup only.

## 7. `IF NOT EXISTS` silently kept old file format settings

**Symptoms:**
- `match_by_column_name option is not supported for file format CSV without PARSE_HEADER = TRUE`
- each claims file loaded as one row holding the whole JSON array
- `Number of columns in file (9) does not match that of the corresponding table (12)`, hidden because `ON_ERROR = 'CONTINUE'` skipped every row and loaded 0 rows without failing

**Cause:** file formats created earlier with different options. `CREATE ... IF NOT EXISTS` did nothing, so the old settings stayed.
**Fix:** `ALTER FILE FORMAT` with `PARSE_HEADER = TRUE`, `STRIP_OUTER_ARRAY = TRUE` and `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE`; then `TRUNCATE` (which also clears COPY's load history) and reload.
**Takeaway:** check `COPY_HISTORY` after loads, since `ON_ERROR = 'CONTINUE'` can hide a complete failure. Keep object definitions in version control so every environment is built the same way.

## 8. Streams created after the data

**Symptom:** every staging procedure reported `0 read`.
**Cause:** after repeated fixes, raw rows were loaded before their streams were recreated. A stream only sees changes made after it was created, and an append-only stream still holds rows that were later truncated.
**Fix:** truncate, recreate the streams, then load. For existing data, `SHOW_INITIAL_ROWS = TRUE` makes a new stream return the current rows once.

## 9. Airflow could not authenticate

**Symptoms:** `The private_key_file path points to an empty or invalid file`, then `Cannot specify basic authenticator together with private key parameters`.
**Causes:** the key had never been copied into the Astro project's `include/keys/`; then the connector defaulted to password authentication.
**Fix:** copy the key file itself (not pasted text), and set `"authenticator": "SNOWFLAKE_JWT"` in the connection extras.
