
-- configures the materialization of our dbt model
-- meaning that the output of this dbt model's SQL code will be stored in a table within our Snowflake database.
{{
    config(
        materialized='table',
        schema='TRANSFORM'
    )
}}

select  MODULE_NAME,
        TOPIC,
        CAST(SUBMISSION_DATE AS DATE) AS SUBMISSION_DATE, 
        CAST(SCORE AS FLOAT) AS SCORE,
        SUBSTRING(EMAIL, 1, CHARINDEX('@', EMAIL) - 1) AS LDAP,
        EMAIL, 
        TEAM,
        PROGRAM,
        COUNTRY,
        EMPLOYEE_NAME,
        CAST(START_DATE AS DATE) AS START_DATE
from {{ ref('initial_load') }} 
where module_name not in('MODULE_NAME')