-- =====================================================================
-- 04  FIRST MANUAL RUN - as the pipeline role, in this order
--     (after the generator has uploaded the first files to S3)
-- =====================================================================
USE ROLE sm_pipeline;
USE SECONDARY ROLES NONE;
USE WAREHOUSE compute_wh;

CALL sentinal_motor.raw.sp_load_raw();
CALL sentinal_motor.staging.sp_stage_policies();     -- policies first: claims and trips check against them
CALL sentinal_motor.staging.sp_stage_claims();
CALL sentinal_motor.staging.sp_stage_telematics();
CALL sentinal_motor.curated.sp_refresh_dim_policy();
CALL sentinal_motor.curated.sp_refresh_fact_claims();
CALL sentinal_motor.curated.sp_refresh_fact_driver_risk();

-- Then run section A of 05_verification.sql.
