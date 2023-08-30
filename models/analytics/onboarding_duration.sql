{{
    config(
        materialized='table',
        schema ='ANALYTICS'
    )
}}

--calculate onboarding duration metrics
SELECT employee_name, start_date, time_to_completion
FROM(
SELECT *, datediff('day', start_date, submission_date) as time_to_completion
FROM(
select *, 
    rank() over(partition by employee_name order by submission_date ASC) as rnk
from {{ ref('clean_raw') }} )
WHERE rnk = 8)
ORDER BY employee_name