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
    select * from {{ source('raw_zone_rndts', 'sorties_statut_dechet') }}
    where
        inserted_at
        = (
            select max(inserted_at)
            from
                {{ source('raw_zone_rndts', 'sorties_statut_dechet') }}
        )
),

renamed as (
    select
        {{ adapter.quote("created_year_utc") }},
        {{ adapter.quote("public_id") }}             as id,
        {{ adapter.quote("identifiant_metier") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("last_modified_date") }},
        {{ adapter.quote("denomination_usuelle") }},
        {{ adapter.quote("code_dechet") }},
        {{ adapter.quote("code_dechet_bale") }},
        {{ adapter.quote("date_utilisation") }},
        {{ adapter.quote("date_expedition") }},
        {{ adapter.quote("nature") }},
        {{ adapter.quote("quantite") }},
        {{ adapter.quote("unite_code") }}            as unite,
        {{ adapter.quote("date_traitement") }},
        {{ adapter.quote("date_fin_traitement") }},
        {{ adapter.quote("code_traitement") }},
        {{ adapter.quote("qualification_code") }},
        {{ adapter.quote("reference_acte_administratif") }},
        {{ adapter.quote("destinataire_type") }},
        {{ adapter.quote("destinataire_numero_identification") }},
        {{ adapter.quote("destinataire_raison_sociale") }},
        {{ adapter.quote("destinataire_adresse_destination") }},
        {{ adapter.quote("destinataire_adresse_libelle") }},
        {{ adapter.quote("destinataire_adresse_commune") }},
        {{ adapter.quote("destinataire_adresse_code_postal") }},
        {{ adapter.quote("destinataire_adresse_pays") }},
        {{ adapter.quote("numero_identification") }} as numero_identification_declarant,
        {{ adapter.quote("sortie_statut_dechet_code_dechet") }},
        {{ adapter.quote("inserted_at") }}

    from source
)

select
    id,
    created_date::timestamptz,
    last_modified_date::timestamptz,
    created_year_utc,
    identifiant_metier,
    numero_identification_declarant,
    denomination_usuelle,
    code_dechet,
    code_dechet_bale,
    date_utilisation::date,
    date_expedition::date,
    nature,
    quantite::numeric,
    unite,
    date_traitement::date,
    date_fin_traitement::date,
    code_traitement,
    qualification_code,
    reference_acte_administratif,
    destinataire_type,
    destinataire_numero_identification,
    destinataire_raison_sociale,
    destinataire_adresse_destination,
    destinataire_adresse_libelle,
    destinataire_adresse_commune,
    destinataire_adresse_code_postal,
    destinataire_adresse_pays,
    sortie_statut_dechet_code_dechet::jsonb
from renamed
