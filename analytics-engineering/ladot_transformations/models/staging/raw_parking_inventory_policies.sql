SELECT
    spaceid,
    blockface,
    metertype,
    ratetype, 
    raterange, 
    timelimit, 
    latlng
FROM {{ source('raw_ladot', 'staging_parking_inventory_policies') }}