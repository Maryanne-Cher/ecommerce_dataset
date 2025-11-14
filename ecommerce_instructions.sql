-- Snowflake user creation
-- Step 1: Use an admin role
USE ROLE ACCOUNTADMIN;


-- Step 2: Create the `transform` role and assign it to ACCOUNTADMIN
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Step 3: Create a default warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Step 5: Create a database and schema for the Ecommerce project
CREATE DATABASE IF NOT EXISTS ECOMMERCE;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE.RAW;

-- Step 6: Grant permissions to the `transform` role
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE ECOMMERCE TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE ECOMMERCE TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE ECOMMERCE TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA ECOMMERCE.RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOMMERCE.RAW TO ROLE TRANSFORM;

-- Step 4: Create the `dbt` user and assign to the transform role
CREATE USER IF NOT EXISTS dbt
  PASSWORD='dbtPassword123'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='ECOMMERCE.RAW'
  COMMENT='DBT user used for data transformation';
ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM TO USER dbt;


CREATE STAGE ecommercestage
  URL='s3://ecommerce-behavior-mkk'
  CREDENTIALS=(AWS_KEY_ID='' AWS_SECRET_KEY='');
**
-- Load raw_data for month of november

CREATE OR REPLACE TABLE raw_ecommerce_nov (
  event_time STRING,
  event_type VARCHAR,
  product_id INTEGER,
  category_id BIGINT,
  category_code VARCHAR,
  brand VARCHAR,
  price FLOAT,
  user_id BIGINT,
  user_session VARCHAR
);

COPY INTO raw_ecommerce_nov
FROM '@ecommercestage/ecommerce_nov.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- load raw data for month of december
CREATE OR REPLACE TABLE raw.raw_ecommerce_dec (
  event_time STRING,
  event_type VARCHAR,
  product_id INTEGER,
  category_id BIGINT,
  category_code VARCHAR,
  brand VARCHAR,
  price FLOAT,
  user_id BIGINT,
  user_session VARCHAR
);

COPY INTO raw_ecommerce_dec
FROM '@ecommercestage/2019-Dec.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- load raw data for month of October
CREATE OR REPLACE TABLE raw.raw_ecommerce_oct (
  event_time STRING,
  event_type VARCHAR,
  product_id INTEGER,
  category_id BIGINT,
  category_code VARCHAR,
  brand VARCHAR,
  price FLOAT,
  user_id BIGINT,
  user_session VARCHAR
);

--load data from s3 bucket
COPY INTO raw_ecommerce_oct
FROM '@ecommercestage/ecommerce_oct.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

