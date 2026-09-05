-- Snowflake MLOps Setup Script
-- Complete infrastructure creation for Feature Store, Model Registry, SPCS, and Lineage Output

-- 1. Create Role & Warehouse Access
CREATE ROLE IF NOT EXISTS ml_role;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE ml_role;

-- 2. Create Database & Schemas
CREATE DATABASE IF NOT EXISTS ml_db;
GRANT OWNERSHIP ON DATABASE ml_db TO ROLE ml_role;
GRANT ROLE ml_role TO USER raghulg;

USE ROLE ml_role;
USE DATABASE ml_db;

CREATE SCHEMA IF NOT EXISTS ml_db.features;
GRANT OWNERSHIP ON SCHEMA ml_db.features TO ROLE ml_role;

CREATE SCHEMA IF NOT EXISTS ml_db.models;
GRANT OWNERSHIP ON SCHEMA ml_db.models TO ROLE ml_role;

CREATE SCHEMA IF NOT EXISTS ml_db.outputs;
GRANT OWNERSHIP ON SCHEMA ml_db.outputs TO ROLE ml_role;

-- 3. Create Raw Data Stage and Table
CREATE STAGE IF NOT EXISTS ml_db.features.raw_stage COMMENT='Stage for raw taxi trip data files';

CREATE OR REPLACE TABLE ml_db.features.raw_data (
    VendorID INTEGER,
    lpep_pickup_datetime TIMESTAMP_NTZ,
    lpep_dropoff_datetime TIMESTAMP_NTZ,
    passenger_count INTEGER,
    trip_distance FLOAT,
    RatecodeID INTEGER,
    store_and_fwd_flag VARCHAR(20),
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    ehail_fee FLOAT,
    trip_type INTEGER,
    cbd_congestion_fee FLOAT
);

-- File Format for Parquet Loading
CREATE OR REPLACE FILE FORMAT ml_db.features.parquet_format
    TYPE = 'PARQUET'
    USE_LOGICAL_TYPE = TRUE;

-- Sample COPY INTO statement (to be run after uploading parquet data to stage)
/*
COPY INTO ml_db.features.raw_data
FROM (
    SELECT $1:VendorID::NUMBER(38, 0), 
    TO_TIMESTAMP_NTZ($1:lpep_pickup_datetime::string), 
    TO_TIMESTAMP_NTZ($1:lpep_dropoff_datetime::string),
    $1:passenger_count::NUMBER(38, 0), 
    $1:trip_distance::FLOAT, 
    $1:RatecodeID::NUMBER(38, 0), 
    $1:store_and_fwd_flag::VARCHAR, 
    $1:PULocationID::NUMBER(38, 0), 
    $1:DOLocationID::NUMBER(38, 0), 
    $1:payment_type::NUMBER(38, 0), 
    $1:fare_amount::FLOAT, 
    $1:extra::FLOAT, 
    $1:mta_tax::FLOAT, 
    $1:tip_amount::FLOAT, 
    $1:tolls_amount::FLOAT, 
    $1:improvement_surcharge::FLOAT, 
    $1:total_amount::FLOAT, 
    $1:congestion_surcharge::FLOAT, 
    $1:ehail_fee::FLOAT, 
    $1:trip_type::NUMBER(38, 0), 
    $1:cbd_congestion_fee::FLOAT
    FROM '@ML_DB.FEATURES.RAW_STAGE'
)
FILES = ('green_tripdata_2025-08.parquet')
FILE_FORMAT = (
    TYPE=PARQUET,
    REPLACE_INVALID_CHARACTERS=TRUE,
    BINARY_AS_TEXT=FALSE
)
ON_ERROR=ABORT_STATEMENT;
*/

-- 4. Snowpark Container Services (SPCS) Infrastructure
-- (Requires ACCOUNTADMIN role for Compute Pool & Image Repository setup)
USE ROLE accountadmin;

-- Create Compute Pool for Model Deployment
CREATE COMPUTE POOL IF NOT EXISTS ml_compute_pool
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = CPU_XSM
    AUTO_SUSPEND_SECS = 3600
    COMMENT = 'Compute Pool for SPCS Model Serving';

GRANT USAGE, MONITOR ON COMPUTE POOL ml_compute_pool TO ROLE ml_role;

-- Create Image Repository for SPCS Docker Containers
CREATE IMAGE REPOSITORY IF NOT EXISTS ml_db.models.spcs_image_repo;
GRANT READ, WRITE ON IMAGE REPOSITORY ml_db.models.spcs_image_repo TO ROLE ml_role;

-- Grant SPCS privileges to ml_role
GRANT CREATE SERVICE ON SCHEMA ml_db.models TO ROLE ml_role;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE ml_role;

USE ROLE ml_role;

-- Verification
SELECT current_role(), current_database(), current_schema();