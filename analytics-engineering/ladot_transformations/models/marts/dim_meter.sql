{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    spaceid AS meter_key,
    metertype AS meter_type,
    ratetype AS rate_type,
    raterange AS rate_range,
    timelimit AS time_limit,
    {{ dbt_utils.generate_surrogate_key([
        'latlng:latitude::DOUBLE', 
        'latlng:longitude::DOUBLE'
    ]) }} AS location_key
FROM {{ ref('raw_parking_inventory_policies') }} 
WHERE spaceid IS NOT NULL