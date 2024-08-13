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
    select * from {{ source('raw_zone_rndts', 'texs_entrant') }}
    where
        inserted_at
        = (
            select max(inserted_at)
            from
                {{ source('raw_zone_rndts', 'texs_entrant') }}
        )
),

renamed as (
    select
        {{ adapter.quote("created_year_utc") }},
        {{ adapter.quote("code_dechet") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("date_reception") }},
        {{ adapter.quote("denomination_usuelle") }},
        {{ adapter.quote("identifiant_terrain_sis") }},
        {{ adapter.quote("last_modified_date") }},
        {{ adapter.quote("numero_document") }},
        {{ adapter.quote("numero_notification") }},
        {{ adapter.quote("numero_saisie") }},
        {{ adapter.quote("quantite") }},
        {{ adapter.quote("is_tex_pop") }},
        {{ adapter.quote("code_traitement") }},
        {{ adapter.quote("unite_code") }}            as unite,
        {{ adapter.quote("numero_bordereau") }},
        {{ adapter.quote("public_id") }}             as id,
        {{ adapter.quote("coordonnees_geographiques") }},
        {{ adapter.quote("coordonnees_geographiques_valorisee") }},
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
        {{ adapter.quote("courtier_type") }},
        {{ adapter.quote("courtier_numero_identification") }},
        {{ adapter.quote("courtier_raison_sociale") }},
        {{ adapter.quote("courtier_numero_recepisse") }},
        {{ adapter.quote("numero_identification") }} as numero_identification_declarant,
        {{ adapter.quote("texs_entrant_transporteur") }},
        {{ adapter.quote("texs_entrant_parcelle_cadastrale") }},
        {{ adapter.quote("texs_entrant_parcelle_valorisee") }}

    from source
)

select
    id,
    created_date::timestamptz,
    last_modified_date::timestamptz,
    created_year_utc::int,
    numero_identification_declarant,
    code_dechet,
    date_reception::date,
    denomination_usuelle,
    identifiant_terrain_sis,
    numero_document,
    numero_notification,
    numero_saisie,
    quantite::numeric,
    is_tex_pop,
    code_traitement,
    unite,
    numero_bordereau,
    coordonnees_geographiques,
    coordonnees_geographiques_valorisee,
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
    courtier_type,
    courtier_numero_identification,
    courtier_raison_sociale,
    courtier_numero_recepisse,
    texs_entrant_transporteur::jsonb,
    texs_entrant_parcelle_cadastrale::jsonb,
    texs_entrant_parcelle_valorisee::jsonb,
    string_to_array(regexp_replace(
        (
            texs_entrant_transporteur::jsonb
            -> 'transporteur_numero_identification'
        )::text,
        '\[? ?"]?',
        '',
        'g'
    ),
    ',') as numeros_indentification_transporteurs
from renamed
