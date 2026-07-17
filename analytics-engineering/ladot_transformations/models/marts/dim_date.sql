{{ config(
    materialized='table'
) }}

WITH all_operational_days AS (
    SELECT DISTINCT
        CAST(eventtime AS DATE) AS calendar_date
    FROM {{ ref('raw_meter_occupancy') }}
    WHERE eventtime IS NOT NULL

    UNION

    SELECT DISTINCT
        CAST(plate_expiry_date AS DATE) AS calendar_date
    FROM {{ ref('raw_parking_citations') }}
    WHERE plate_expiry_date IS NOT NULL

    UNION

    SELECT DISTINCT
        CAST(issue_date AS DATE) AS calendar_date
    FROM {{ ref('raw_parking_citations') }}
    WHERE issue_date IS NOT NULL
)

SELECT DISTINCT
    calendar_date AS date_key,
    EXTRACT(DAY FROM calendar_date) AS day,
    EXTRACT(MONTH FROM calendar_date) AS month,
    EXTRACT(YEAR FROM calendar_date) AS year,
    DAYNAME(calendar_date) AS day_of_week,
    MONTHNAME(calendar_date) AS month_name
FROM all_operational_days
WHERE calendar_date IS NOT NULL