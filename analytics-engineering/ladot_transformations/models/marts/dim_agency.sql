{{ config(
    materialized='table'
) }}

SELECT DISTINCT 
    agency  AS agency_key,
    agency_desc AS description
FROM {{ ref('raw_parking_citations') }} 
WHERE agency IS NOT NULL 