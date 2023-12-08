with source as (
    select * from {{ source('raw_zone_trackdechets', 'anonymous_company_raw') }}
),

renamed as (
    select
        id,
        siret,
        name,
        address,
        codenaf     as code_naf,
        libellenaf  as libelle_naf,
        codecommune as code_commune,
        vatnumber   as vat_number,
        orgid       as org_id
    from source
)

select * from renamed
