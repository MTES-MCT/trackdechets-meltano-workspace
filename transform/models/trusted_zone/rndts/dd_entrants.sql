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
    select * from {{ source('raw_zone_rndts', 'dd_entrants') }}
    where
        inserted_at
        = (
            select max(inserted_at)
            from
                {{ source('raw_zone_rndts', 'dd_entrants') }}
        )
),

renamed as (
    select
        {{ adapter.quote("created_year_utc") }},
        {{ adapter.quote("public_id") }}             as id,
        {{ adapter.quote("code_dechet") }},
        {{ adapter.quote("code_dechet_bale") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("date_reception") }},
        {{ adapter.quote("is_dechet_pop") }},
        {{ adapter.quote("denomination_usuelle") }},
        {{ adapter.quote("heure_pesee") }},
        {{ adapter.quote("last_modified_date") }},
        {{ adapter.quote("numero_document") }},
        {{ adapter.quote("numero_notification") }},
        {{ adapter.quote("numero_saisie") }},
        {{ adapter.quote("quantite") }},
        {{ adapter.quote("code_traitement") }},
        {{ adapter.quote("unite_code") }},
        {{ adapter.quote("identifiant_metier") }},
        {{ adapter.quote("producteur_type") }},
        {{ adapter.quote("producteur_numero_identification") }},
        {{ adapter.quote("producteur_raison_sociale") }},
        {{ adapter.quote("producteur_adresse_libelle") }},
        {{ adapter.quote("producteur_adresse_commune") }},
        {{ adapter.quote("producteur_adresse_code_postal") }},
        {{ adapter.quote("producteur_adresse_pays") }},
        {{ adapter.quote("expediteur_type") }},
        {{ adapter.quote("expediteur_numero_identification") }},
        {{ adapter.quote("expediteur_raison_sociale") }},
        {{ adapter.quote("expediteur_adresse_prise_en_charge") }},
        {{ adapter.quote("expediteur_adresse_libelle") }},
        {{ adapter.quote("expediteur_adresse_commune") }},
        {{ adapter.quote("expediteur_adresse_code_postal") }},
        {{ adapter.quote("expediteur_adresse_pays") }},
        {{ adapter.quote("eco_organisme_type") }},
        {{ adapter.quote("eco_organisme_numero_identification") }},
        {{ adapter.quote("eco_organisme_raison_sociale") }},
        {{ adapter.quote("courtier_type") }},
        {{ adapter.quote("courtier_numero_identification") }},
        {{ adapter.quote("courtier_raison_sociale") }},
        {{ adapter.quote("courtier_numero_recepisse") }},
        {{ adapter.quote("numero_identification") }} as numero_identification_declarant,
        {{ adapter.quote("dd_entrant_transporteur") }},
        {{ adapter.quote("dd_entrant_commune") }},
        {{ adapter.quote("inserted_at") }}

    from source
)

select
    id,
    created_date::timestamptz,
    last_modified_date::timestamptz,
    created_year_utc::int,
    numero_identification_declarant,
    code_dechet,
    code_dechet_bale,
    date_reception::date,
    is_dechet_pop,
    denomination_usuelle,
    heure_pesee::time,
    numero_document,
    numero_notification,
    numero_saisie,
    quantite::numeric,
    code_traitement,
    unite_code,
    identifiant_metier,
    producteur_type,
    producteur_numero_identification,
    producteur_raison_sociale,
    producteur_adresse_libelle,
    producteur_adresse_commune,
    producteur_adresse_code_postal,
    producteur_adresse_pays,
    expediteur_type,
    expediteur_numero_identification,
    expediteur_raison_sociale,
    expediteur_adresse_prise_en_charge,
    expediteur_adresse_libelle,
    expediteur_adresse_commune,
    expediteur_adresse_code_postal,
    expediteur_adresse_pays,
    eco_organisme_type,
    eco_organisme_numero_identification,
    eco_organisme_raison_sociale,
    courtier_type,
    courtier_numero_identification,
    courtier_raison_sociale,
    courtier_numero_recepisse,
    dd_entrant_transporteur::jsonb,
    dd_entrant_commune::jsonb,
    inserted_at
from renamed
