SELECT
    id,
    receiptnumber as receipt_number,
    validitylimit as validity_limit,
    department
FROM
    {{ source('raw_zone_trackdechets', 'broker_receipt_raw') }}
