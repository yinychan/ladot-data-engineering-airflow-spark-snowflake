{{ config(
    materialized='table'
) }}

WITH cleaned_citation_source AS (
    SELECT
        ticket_number,
        fine_amount,
        agency AS agency_key,
        meter_id AS meter_key,
        
        CAST(issue_date AS DATE) AS date_key,
        LPAD(REGEXP_REPLACE(issue_time, '[^0-9]', ''), 4, '0') AS issue_time,

        loc_lat AS latitude,
        loc_long AS longitude,

        rp_state_plate,
        plate_expiry_date,
        make,
        body_style_desc,
        color_desc,
        violation_code,
        violation_description
    FROM {{ ref('raw_parking_citations') }}
)

SELECT 
    ticket_number,
    fine_amount,
    date_key,
    agency_key,
    meter_key,
    TIME_FROM_PARTS(
        CAST(SUBSTRING(issue_time, 1, 2) AS INT),
        CAST(SUBSTRING(issue_time, 3, 2) AS INT),
        0
    ) AS time_of_day_key,
    {{ dbt_utils.generate_surrogate_key([
        "COALESCE(NULLIF(TRIM(violation_code), ''), 'UNKNOWN CODE')",
        "COALESCE(NULLIF(TRIM(violation_description), ''), 'UNKNOWN DESCRIPTION')"
    ]) }} AS violation_key,
    CASE 
        WHEN latitude IS NULL OR longitude IS NULL OR latitude = 0 OR longitude = 0 THEN 'UNKNOWN_LOCATION'
        ELSE {{ dbt_utils.generate_surrogate_key(['latitude', 'longitude']) }}
    END AS location_key,
    {{ dbt_utils.generate_surrogate_key([
        'rp_state_plate',
        'plate_expiry_date',
        'make',
        'body_style_desc',
        'color_desc'
    ]) }} AS vehicle_key
FROM cleaned_citation_source