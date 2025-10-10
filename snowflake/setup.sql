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
        */
create or replace table ml_db.features.raw_data (
    VendorID integer,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count integer,
    trip_distance float,
    RatecodeID integer,
    store_and_fwd_flag string,
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
    airport_fee float
);

create or replace file format ml_db.features.parquet_format
type = 'PARQUET';
copy into ml_db.features.raw_data
from (select $1:VendorID::integer,
             $1:lpep_pickup_datetime::timestamp,
             $1:lpep_dropoff_datetime::timestamp,
             $1:passenger_count::integer,
             $1:trip_distance::float,
             $1:RatecodeID::integer,
             $1:store_and_fwd_flag::string,
             $1:PULocationID::integer,
             $1:DOLocationID::integer,
             $1:payment_type::integer,
             $1:fare_amount::float,
             $1:extra::float,
             $1:mta_tax::float,
             $1:tip_amount::float,
             $1:tolls_amount::float,
             $1:improvement_surcharge::float,
             $1:total_amount::float,
             $1:congestion_surcharge::float,
             $1:airport_fee::float
      from @ml_db.features.raw_stage/green_tripdata_2025-08.parquet);
-- validate the data load
select count(*) from ml_db.features.raw_data;