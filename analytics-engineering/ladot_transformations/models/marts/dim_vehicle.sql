{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key([
        'rp_state_plate', 
        'plate_expiry_date', 
        'make',
        'body_style_desc',
        'color_desc'
    ]) }} as vehicle_key, 
    rp_state_plate, 
    plate_expiry_date, 
    make, 
    body_style_desc, 
    color_desc 
FROM {{ ref('raw_parking_citations') }} 
WHERE rp_state_plate IS NOT NULL 
  AND plate_expiry_date IS NOT NULL