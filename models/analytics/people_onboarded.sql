WITH team_counts  
AS(
select 'TEAM' AS TYPE, TEAM AS VAL, count(distinct EMAIL) as cnt
from {{ ref('clean_raw') }} 
GROUP BY 1,2), 

program_counts as(
select 'PROGRAM' AS TYPE, PROGRAM AS VAL, count(distinct EMAIL) as cnt
from {{ ref('clean_raw') }} 
GROUP BY 1,2),

country_counts as(
select 'COUNTRY' AS TYPE, COUNTRY as VAL,  count(distinct EMAIL) as cnt
from {{ ref('clean_raw') }} 
GROUP BY 1,2)   

{{
    config(
        materialized='table',
        schema ='ANALYTICS'
    )
}}

select *
from team_counts
UNION ALL 
select *
from program_counts
UNION ALL
select *
from country_counts


