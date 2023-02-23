SELECT
    id,
    receiptnumber as receipt_number,
    validitylimit as validity_limit,
    department
FROM
    {{ source('raw_zone_trackdechets', 'transporter_receipt_raw') }}