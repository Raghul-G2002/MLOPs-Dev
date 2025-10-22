--Create a new role called ml_role
create role ml_role;
grant usage on warehouse compute_wh to role ml_role;

--Create database, and schemas for features, models and outputs
create database if not exists ml_db;
grant ownership on database ml_db to role ml_role;
grant role ml_role to user raghulg;
use role ml_role;

create schema if not exists ml_db.features;
grant ownership on schema ml_db.features to role ml_role;
create schema if not exists ml_db.models;
grant ownership on schema ml_db.models to role ml_role;

create schema if not exists ml_db.outputs;
grant ownership on schema ml_db.outputs to role ml_role;

-- push the parquet files and load into a table in the features schema
create stage if not exists ml_db.features.raw_stage comment='stage for raw data files';
-- Once pushing the parquet files to the stage, run the below copy command to load data into a table
/*
'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime',
       'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID',
       'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',
       'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge',
       'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge',
       'cbd_congestion_fee']
VendorID int32
lpep_pickup_datetime datetime64[us]
lpep_dropoff_datetime datetime64[us]
store_and_fwd_flag object
RatecodeID float64
PULocationID int32
DOLocationID int32
passenger_count float64
trip_distance float64
fare_amount float64
extra float64
mta_tax float64
tip_amount float64
tolls_amount float64
ehail_fee float64
improvement_surcharge float64
total_amount float64
payment_type float64
trip_type float64
congestion_surcharge float64
cbd_congestion_fee float64
        */

drop table if exists ml_db.features.raw_data;
create or replace table ml_db.features.raw_data (
    VendorID integer,
    lpep_pickup_datetime timestamp_ntz,
    lpep_dropoff_datetime timestamp_ntz,
    passenger_count integer,
    trip_distance float,
    RatecodeID integer,
    store_and_fwd_flag varchar(20),
    PULocationID integer,
    DOLocationID integer,
    payment_type integer,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    improvement_surcharge float,
    total_amount float,
    congestion_surcharge float,
    ehail_fee float,
    trip_type integer,
    cbd_congestion_fee float
);

create or replace file format ml_db.features.parquet_format
type = 'PARQUET'
use_logical_type = true;
copy into ml_db.features.raw_data
from (
    -- TO_TIMESTAMP_NTZ($1:timestamp_col::string)
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
    FROM '@"ML_DB"."FEATURES"."RAW_STAGE"'
)
FILES = ('green_tripdata_2025-08.parquet')
FILE_FORMAT = (
    TYPE=PARQUET,
    REPLACE_INVALID_CHARACTERS=TRUE,
    BINARY_AS_TEXT=FALSE
)
ON_ERROR=ABORT_STATEMENT;
-- validate the data load

select * from ml_db.features.raw_data limit 5;
select count(*) from ml_db.features.raw_data;

-- alter the table - convert the datetime columns from integer to timestamp\
alter table ml_db.features.raw_data
drop column lpep_pickup_datetime_new;
alter table ml_db.features.raw_data
add column lpep_pickup_datetime_new timestamp;
update ml_db.features.raw_data
set lpep_pickup_datetime_new = to_timestamp_(lpep_pickup_datetime);
alter table ml_db.features.raw_data
alter column lpep_dropoff_datetime set data type timestamp_ntz;