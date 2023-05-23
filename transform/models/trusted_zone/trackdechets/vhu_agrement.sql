SELECT
    id,
    agrementnumber AS agrement_number,
    department
FROM
    {{ source('raw_zone_trackdechets', 'vhu_agrement_raw') }}
