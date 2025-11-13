{{ config(
    materialized='table'
) }}

-- Generate date range from 2019-10-01 to 2020-04-30
WITH date_range AS (
    SELECT 
        '2019-10-01'::DATE AS start_date,
        '2020-04-30'::DATE AS end_date
),
dates AS (
    SELECT 
        DATEADD(DAY, SEQ4(), start_date) AS full_date
    FROM date_range,
         TABLE(GENERATOR(ROWCOUNT => 213)) -- 213 days between Oct 1, 2019 and Apr 30, 2020
)

SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS datekey,
    full_date AS fulldate,
    YEAR(full_date) AS yearnum,
    MONTH(full_date) AS monthnum,
    INITCAP(TO_CHAR(full_date, 'Month')) AS monthname,
    DAY(full_date) AS daynum,
    INITCAP(TO_CHAR(full_date, 'Day')) AS dayofweekname,
    DAYOFWEEKISO(full_date) AS dayofweeknum,  -- 1 = Monday, 7 = Sunday
    QUARTER(full_date) AS quarternum,
    WEEKISO(full_date) AS weeknum,  -- ISO week number
    CASE 
        WHEN MONTH(full_date) IN (12,1,2) THEN 'Winter'
        WHEN MONTH(full_date) IN (3,4,5)  THEN 'Spring'
        WHEN MONTH(full_date) IN (6,7,8)  THEN 'Summer'
        ELSE 'Fall'
    END AS season,
    CASE 
        WHEN full_date = '2019-10-31' THEN 'Halloween'
        WHEN full_date = '2019-11-28' THEN 'Thanksgiving'
        WHEN full_date = '2019-12-25' THEN 'Christmas'
        WHEN full_date = '2020-01-01' THEN 'New Year''s Day'
        WHEN full_date = '2020-02-14' THEN 'Valentine''s Day'
        WHEN full_date = '2020-04-12' THEN 'Easter'
        ELSE NULL
    END AS holiday,
    CASE WHEN DAYOFWEEKISO(full_date) IN (6,7) THEN TRUE ELSE FALSE END AS isweekend,
    CONCAT(YEAR(full_date), '-Q', QUARTER(full_date)) AS yearquarter,
    TO_NUMBER(CONCAT(YEAR(full_date), QUARTER(full_date))) AS yearquartersort,
    CONCAT(TO_CHAR(full_date, 'Mon'), '-', YEAR(full_date)) AS monthyear,
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMM')) AS monthyearsort
FROM dates
ORDER BY full_date
