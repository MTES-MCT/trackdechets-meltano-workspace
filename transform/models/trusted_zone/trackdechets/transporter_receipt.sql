SELECT
    id,
    receiptnumber AS receipt_number,
    validitylimit AS validity_limit,
    department
FROM
    {{ source('raw_zone_trackdechets', 'transporter_receipt_raw') }}
