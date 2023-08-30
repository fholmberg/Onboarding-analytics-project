{{
    config(
        materialized='table',
        schema ='ANALYTICS'
    )
}}

SELECT MODULE_NAME, AVG(duration) AS AVG_DUR_BY_MODULE
FROM(
SELECT module_name, employee_name, submission_date, next_submission_date,
    CASE 
        WHEN module_name IN('HR') then 1
        else duration
        end as duration
FROM(
SELECT *, datediff('day', next_submission_date, submission_date) as duration
FROM(
SELECT module_name, employee_name, submission_date,
    lag(submission_date) OVER(PARTITION BY employee_name ORDER BY submission_date ASC) AS next_submission_date
from {{ ref('clean_raw') }})
ORDER BY employee_name))
GROUP BY 1
ORDER BY 2 DESC