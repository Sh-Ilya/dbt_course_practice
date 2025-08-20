{{
        config(
            materialized = 'table'
        )
}}

SELECT 
aircraft_code
, model
, "range"
FROM {{ source('demo_src', 'aircrafts') }}