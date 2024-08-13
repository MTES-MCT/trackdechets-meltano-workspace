{{
  config(
    materialized = 'table',
    indexes=[
        {"columns":['id'],"unique":True},
        {"columns":['created_date']},
        {"columns":['numero_identification_declarant']},
        {"columns":['numeros_indentification_transporteurs'],"type":"GIN"},
        ]

    )
}}

with source as (
    select * from {{ source('raw_zone_rndts', 'dnd_entrant') }}
    where
        inserted_at
        = (
            select max(inserted_at)
            from
                {{ source('raw_zone_rndts', 'dnd_entrant') }}
        )
),

renamed as (
    select
        {{ adapter.quote("created_year_utc") }},
        {{ adapter.quote("code_dechet") }},
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
        {{ adapter.quote("unite_code") }}            as unite,
        {{ adapter.quote("public_id") }}             as id,
        {{ adapter.quote("code_dechet_bale") }},
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
        {{ adapter.quote("dnd_entrant_transporteur") }},
        {{ adapter.quote("dnd_entrant_commune") }}

    from source
)

select
    id,
    created_date::date,
    last_modified_date::timestamptz,
    created_year_utc::int,
    numero_identification_declarant,
    code_dechet,
    date_reception::date,
    is_dechet_pop,
    denomination_usuelle,
    heure_pesee::time,
    numero_document,
    numero_notification,
    numero_saisie,
    quantite::numeric,
    code_traitement,
    unite,
    code_dechet_bale,
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
    dnd_entrant_transporteur,
    dnd_entrant_commune,
    string_to_array(regexp_replace(
        (
            dnd_entrant_transporteur::jsonb
            -> 'transporteur_numero_identification'
        )::text,
        '\[? ?"]?',
        '',
        'g'
    ),
    ',') as numeros_indentification_transporteurs
from renamed
