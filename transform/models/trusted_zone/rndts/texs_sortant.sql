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
    select * from {{ source('raw_zone_rndts', 'texs_sortant') }}
    where
        inserted_at
        = (
            select max(inserted_at)
            from
                {{ source('raw_zone_rndts', 'texs_sortant') }}
        )
),

renamed as (
    select
        {{ adapter.quote("created_year_utc") }},
        {{ adapter.quote("code_dechet") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("date_expedition") }},
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
        {{ adapter.quote("qualification_code") }},
        {{ adapter.quote("code_dechet_bale") }},
        {{ adapter.quote("identifiant_metier") }},
        {{ adapter.quote("producteur_type") }},
        {{ adapter.quote("producteur_numero_identification") }},
        {{ adapter.quote("producteur_raison_sociale") }},
        {{ adapter.quote("producteur_adresse_libelle") }},
        {{ adapter.quote("producteur_adresse_commune") }},
        {{ adapter.quote("producteur_adresse_code_postal") }},
        {{ adapter.quote("producteur_adresse_pays") }},
        {{ adapter.quote("destinataire_type") }},
        {{ adapter.quote("destinataire_numero_identification") }},
        {{ adapter.quote("destinataire_raison_sociale") }},
        {{ adapter.quote("destinataire_adresse_destination") }},
        {{ adapter.quote("destinataire_adresse_libelle") }},
        {{ adapter.quote("destinataire_adresse_commune") }},
        {{ adapter.quote("destinataire_adresse_code_postal") }},
        {{ adapter.quote("destinataire_adresse_pays") }},
        {{ adapter.quote("courtier_type") }},
        {{ adapter.quote("courtier_numero_identification") }},
        {{ adapter.quote("courtier_raison_sociale") }},
        {{ adapter.quote("courtier_numero_recepisse") }},
        {{ adapter.quote("numero_identification") }} as numero_identification_declarant,
        {{ adapter.quote("texs_sortant_transporteur") }},
        {{ adapter.quote("texs_sortant_parcelle_cadastrale") }},
        {{ adapter.quote("texs_sortant_parcelle_valorisee") }}
    from source
)

select
    id,
    created_date::timestamptz,
    last_modified_date::timestamptz,
    created_year_utc,
    numero_identification_declarant,
    code_dechet,
    date_expedition::date,
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
    qualification_code,
    code_dechet_bale,
    identifiant_metier,
    producteur_type,
    producteur_numero_identification,
    producteur_raison_sociale,
    producteur_adresse_libelle,
    producteur_adresse_commune,
    producteur_adresse_code_postal,
    producteur_adresse_pays,
    destinataire_type,
    destinataire_numero_identification,
    destinataire_raison_sociale,
    destinataire_adresse_destination,
    destinataire_adresse_libelle,
    destinataire_adresse_commune,
    destinataire_adresse_code_postal,
    destinataire_adresse_pays,
    courtier_type,
    courtier_numero_identification,
    courtier_raison_sociale,
    courtier_numero_recepisse,
    texs_sortant_transporteur::jsonb,
    texs_sortant_parcelle_cadastrale::jsonb,
    texs_sortant_parcelle_valorisee::jsonb
from renamed
