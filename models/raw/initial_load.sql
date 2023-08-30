{{
    config(
        materialized='table',
        schema = 'RAW'
    )
}}

select *
from {{ source('PUBLIC', 'ONBOARDING_DWH') }}