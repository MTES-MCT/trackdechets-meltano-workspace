{{
  config(
    materialized = 'table',
    indexes=[
        {"columns":['id'],"unique":True},
        {"columns":['created_date']},
        {"columns":['numero_identification_declarant']},
        ]

    )
}}

with source as (
    select * from {{ source('raw_zone_rndts', 'dd_sortants') }}
),

renamed as (
    select
        {{ adapter.quote("declarant.numero_identification") }} as numero_identification_declarant,
        {{ adapter.quote("declarant.raison_sociale") }}        as raison_sociale_declarant,
        {{ adapter.quote("created_year_utc") }},
        {{ adapter.quote("id") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("last_modified_date") }},
        {{ adapter.quote("date_expedition") }},
        {{ adapter.quote("is_dechet_pop") }}                   as dechet_pop,
        {{ adapter.quote("quantite") }},
        {{ adapter.quote("unite") }},
        {{ adapter.quote("code_dechet") }},
        {{ adapter.quote("denomination_usuelle") }},
        {{ adapter.quote("status") }}
    from source
)

select
    id::integer,
    created_date::timestamptz,
    created_year_utc::smallint,
    last_modified_date::timestamptz,
    numero_identification_declarant,
    raison_sociale_declarant,
    code_dechet,
    dechet_pop,
    denomination_usuelle,
    quantite::numeric,
    unite,
    date_expedition::date,
    status
from renamed
