SELECT
    id,
    receiptnumber AS receipt_number,
    validitylimit AS validity_limit,
    department
FROM
    {{ source('raw_zone_trackdechets', 'trader_receipt_raw') }}
