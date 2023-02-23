SELECT
    id,
    agrementnumber,
    department
FROM
    {{ source('raw_zone_trackdechets', 'vhu_agrement_raw') }}