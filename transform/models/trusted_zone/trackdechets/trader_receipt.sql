SELECT
    id,
    receiptnumber as receipt_number,
    validitylimit as validity_limit,
    department
FROM
    {{ source('raw_zone_trackdechets', 'trader_receipt_raw') }}
