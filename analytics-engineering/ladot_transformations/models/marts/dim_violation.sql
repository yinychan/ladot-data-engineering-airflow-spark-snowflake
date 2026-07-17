{{ config(
    materialized='table'
) }}

WITH standardized_violation_codes AS (
    SELECT DISTINCT
        COALESCE(
            NULLIF(TRIM(violation_code), ''),
            'UNKNOWN CODE'
        ) AS violation_code,
        COALESCE(
            NULLIF(TRIM(violation_description), ''),
            'UNKNOWN DESCRIPTION'
        ) AS violation_description
    FROM {{ ref('raw_parking_citations') }}
)

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key([
        'violation_code',
        'violation_description'
    ]) }} AS violation_key,
    violation_code AS code,
    violation_description AS description
FROM standardized_violation_codes