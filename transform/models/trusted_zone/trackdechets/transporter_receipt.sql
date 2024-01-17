{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["id"], "unique": True}
    ]
    )
}}

with source as (
    select *
    from {{ source('raw_zone_trackdechets', 'transporter_receipt_raw') }}
),

renamed as (
    select
        id,
        "receiptNumber" as receipt_number,
        "validityLimit" as validity_limit,
        department
    from
        source
    where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
)

select
    id,
    receipt_number,
    validity_limit,
    department
from renamed
