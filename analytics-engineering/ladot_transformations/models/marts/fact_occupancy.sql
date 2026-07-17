{{ config(
    materialized='table'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'spaceid',
        'eventtime'
    ]) }} AS occupancy_key,
    
    spaceid AS meter_key,
    
    CAST(eventtime AS DATE) AS date_key,
    
    CAST(eventtime AS TIME) AS time_of_day_key
FROM {{ ref('raw_meter_occupancy') }}
WHERE spaceid IS NOT NULL 
  AND eventtime IS NOT NULL