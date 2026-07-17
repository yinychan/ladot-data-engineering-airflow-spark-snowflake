{{ config(
    materialized='table'
) }}

    -- - location_key
    -- - address_number_street (from fields location and blockface)
    -- - latitude
    -- - longitude

-- 1. collect all the coordinates in the city, and union between raw_parking_citations and raw_parking_inventory_policies
-- 1a. The SQL WITH clause is used to create a temporary table called combined_coordinates that combines the latitude and longitude coordinates from two different sources: raw_parking_citations and raw_parking_inventory_policies. The UNION ALL operator is used to combine the results of the two SELECT statements, which retrieve the latitude and longitude values from each source. The resulting combined_coordinates table will contain all the unique latitude and longitude pairs from both sources, which can then be used for further analysis or processing.
WITH combined_coordinates AS (
    SELECT DISTINCT
        loc_lat AS latitude,
        loc_long AS longitude,
        location AS raw_address,
        'citation' AS source_origin
    FROM {{ ref('raw_parking_citations') }} 
    WHERE loc_lat IS NOT NULL AND loc_long IS NOT NULL
      AND loc_lat != 0 AND loc_long != 0

    UNION ALL

    SELECT DISTINCT
        latlng:latitude::DOUBLE AS latitude,
        latlng:longitude::DOUBLE AS longitude,
        blockface AS raw_address,
        'policy' AS source_origin
    FROM {{ ref('raw_parking_inventory_policies') }}
    WHERE latlng:latitude::DOUBLE IS NOT NULL AND latlng:longitude::DOUBLE IS NOT NULL
      AND latlng:latitude::DOUBLE != 0 AND latlng:longitude::DOUBLE != 0

),
-- 2. rank the coordinates by source_origin, and prioritize the policy source over the citation source. The ROW_NUMBER() function is used to assign a unique row number to each coordinate pair based on the order of the source_origin column. The PARTITION BY clause is used to group the coordinates by latitude and longitude, so that each unique coordinate pair is assigned a row number starting from 1. The ORDER BY clause is used to prioritize the policy source over the citation source, so that if a coordinate pair appears in both sources, the policy source will be assigned a lower row number (i.e., higher priority) than the citation source. The resulting ranked_coordinates table will contain all the unique latitude and longitude pairs from both sources, along with their corresponding raw addresses and source origins, and will be sorted by priority based on the source_origin column.
ranked_coordinates AS (
    SELECT
        latitude,
        longitude,
        raw_address,
        source_origin, 
        ROW_NUMBER() OVER (
            PARTITION BY latitude, longitude 
            ORDER BY CASE WHEN source_origin = 'policy' THEN 1 ELSE 2 END
        ) AS address_rank
    FROM combined_coordinates
)

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key([
        'a.latitude', 
        'a.longitude'
    ]) }} AS location_key,
    COALESCE(r.raw_address, 'UNKNOWN ADDRESS') AS address_number_street,
    a.latitude,
    a.longitude
FROM combined_coordinates a 
LEFT JOIN ranked_coordinates r
    ON a.latitude = r.latitude 
    AND a.longitude = r.longitude 
    AND r.address_rank = 1  