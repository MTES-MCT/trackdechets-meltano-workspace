{{
  config(
    materialized = 'table',
    indexes = [
        { "columns" : ["id"], "unique": True },
    ]
    )
}}
with source as (
    select * from {{ source('raw_zone_trackdechets', 'broker_receipt_raw') }}
)
SELECT
    id,
    "receiptNumber" AS receipt_number,
    "validityLimit" AS validity_limit,
    department
FROM
    source
where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)