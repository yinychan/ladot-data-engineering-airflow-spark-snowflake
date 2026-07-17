{{ config(
    materialized='table'
) }}

WITH cleaned_citation_times AS (
    SELECT DISTINCT
        LPAD(REGEXP_REPLACE(issue_time, '[^0-9]', ''), 4, '0') AS issue_time
    FROM {{ ref('raw_parking_citations') }}
    WHERE issue_time IS NOT NULL
      AND TRIM(issue_time) != ''
),

all_operational_times AS (
    SELECT DISTINCT
        CAST(eventtime AS TIME) AS time_of_day
    FROM {{ ref('raw_meter_occupancy') }}
    WHERE eventtime IS NOT NULL

    UNION

    SELECT DISTINCT
        TIME_FROM_PARTS(
            CAST(SUBSTRING(issue_time, 1, 2) AS INT),
            CAST(SUBSTRING(issue_time, 3, 2) AS INT),
            0
        ) AS time_of_day
    FROM cleaned_citation_times
    WHERE CAST(SUBSTRING(issue_time, 1, 2) AS INT) BETWEEN 0 AND 23
      AND CAST(SUBSTRING(issue_time, 3, 2) AS INT) BETWEEN 0 AND 59
)

SELECT DISTINCT
    time_of_day AS time_of_day_key,
    EXTRACT(HOUR FROM time_of_day) AS hour,
    EXTRACT(MINUTE FROM time_of_day) AS minute
FROM all_operational_times
WHERE time_of_day IS NOT NULL