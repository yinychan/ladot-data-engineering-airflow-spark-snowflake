SELECT
    spaceid,
    eventtime,
    occupancystate
FROM {{ source('raw_ladot', 'staging_meter_occupancy') }}