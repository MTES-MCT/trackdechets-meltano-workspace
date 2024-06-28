{{ config(
  pre_hook = "{{ create_indexes_for_source([
    'Code AIOT',
    'SIRET',
    'Numéro rubrique'
    ]) }}"
) }}

with source as (
    select *
    from {{ source('raw_zone_icpe', 'installations_rubriques_2024_raw') }}
    where
        inserted_at
        = (
            select max(inserted_at)
            from
                {{ source('raw_zone_icpe', 'installations_rubriques_2024_raw') }}
        )
),

renamed as (
    select
        "Raison sociale/nom"                as raison_sociale,
        "SIRET"                             as siret,
        "Code AIOT"                         as code_aiot,
        "Etat du site (code)"               as code_etat_site,
        "Etat du site (libellé)"            as libelle_etat_site,
        "Numéro rubrique"                   as rubrique,
        "Régime"                            as regime,
        "Quantité projet"                   as quantite_projet,
        "Quantité totale"                   as quantite_totale,
        "Unité"                             as unite,
        "Etat technique de la rubrique"     as etat_technique_rubrique,
        "Etat administratif de la rubrique" as etat_administratif_rubrique
    from source
)

select
    raison_sociale,
    siret,
    code_aiot,
    code_etat_site,
    libelle_etat_site,
    regime,
    quantite_projet,
    quantite_totale,
    unite,
    etat_technique_rubrique,
    etat_administratif_rubrique,
    replace(rubrique, '.', '-') as rubrique
from renamed
