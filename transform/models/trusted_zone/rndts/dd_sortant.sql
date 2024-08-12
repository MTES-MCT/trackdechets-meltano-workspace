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
    select * from {{ source('raw_zone_rndts', 'dd_sortant') }}
    where
        inserted_at
        = (
            select max(inserted_at)
            from
                {{ source('raw_zone_rndts', 'dd_sortant') }}
        )
),

renamed as (
    select
        {{ adapter.quote("created_year_utc") }},
        {{ adapter.quote("public_id") }}             as id,
        {{ adapter.quote("code_dechet") }},
        {{ adapter.quote("code_dechet_bale") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("date_expedition") }},
        {{ adapter.quote("is_dechet_pop") }},
        {{ adapter.quote("denomination_usuelle") }},
        {{ adapter.quote("last_modified_date") }},
        {{ adapter.quote("numero_document") }},
        {{ adapter.quote("numero_notification") }},
        {{ adapter.quote("numero_saisie") }},
        {{ adapter.quote("quantite") }},
        {{ adapter.quote("qualification_code") }},
        {{ adapter.quote("code_traitement") }},
        {{ adapter.quote("unite_code") }}            as unite,
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
        {{ adapter.quote("etablissement_origine_id") }},
        {{ adapter.quote("etablissement_origine_adresse_prise_en_charge") }},
        {{ adapter.quote("etablissement_origine_adresse_libelle") }},
        {{ adapter.quote("etablissement_origine_adresse_commune") }},
        {{ adapter.quote("etablissement_origine_adresse_code_postal") }},
        {{ adapter.quote("etablissement_origine_adresse_pays") }},
        {{ adapter.quote("eco_organisme_type") }},
        {{ adapter.quote("eco_organisme_numero_identification") }},
        {{ adapter.quote("eco_organisme_raison_sociale") }},
        {{ adapter.quote("courtier_type") }},
        {{ adapter.quote("courtier_numero_identification") }},
        {{ adapter.quote("courtier_raison_sociale") }},
        {{ adapter.quote("courtier_numero_recepisse") }},
        {{ adapter.quote("numero_identification") }} as numero_identification_declarant,
        {{ adapter.quote("dd_sortant_transporteur") }},
        {{ adapter.quote("dd_sortant_commune") }}

    from source
)

select
    id,
    created_date::timestamptz,
    last_modified_date::timestamptz,
    created_year_utc,
    numero_identification_declarant,
    code_dechet,
    code_dechet_bale,
    date_expedition::date,
    is_dechet_pop,
    denomination_usuelle,
    numero_document,
    numero_notification,
    numero_saisie,
    quantite::numeric,
    qualification_code,
    code_traitement,
    unite,
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
    etablissement_origine_id,
    etablissement_origine_adresse_prise_en_charge,
    etablissement_origine_adresse_libelle,
    etablissement_origine_adresse_commune,
    etablissement_origine_adresse_code_postal,
    etablissement_origine_adresse_pays,
    eco_organisme_type,
    eco_organisme_numero_identification,
    eco_organisme_raison_sociale,
    courtier_type,
    courtier_numero_identification,
    courtier_raison_sociale,
    courtier_numero_recepisse,
    dd_sortant_transporteur::jsonb,
    dd_sortant_commune::jsonb,
    string_to_array(regexp_replace((dd_sortant_transporteur::jsonb->'transporteur_numero_identification')::text,
	'\[? ?"]?',
	'',
	'g'),
	',') as numeros_indentification_transporteurs
from renamed
