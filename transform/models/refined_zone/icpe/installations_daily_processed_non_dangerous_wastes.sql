{{
  config(
    materialized = 'table',
    indexes=[{"columns":["siret","day_of_processing","rubrique"], "unique":True}],
    tags =  ["fiche-etablissements"]
    )
}}

with installations as (
    select
        siret,
        regexp_replace(rubrique, '\-b|\-a', '') as rubrique,
        max(raison_sociale)                     as raison_sociale,
        array_agg(distinct code_aiot)           as codes_aiot,
        sum(quantite_totale)                    as quantite_autorisee
    from
        {{ ref('installations_rubriques_2024') }}
    where
        siret is not null
        and (
            rubrique in ('2771', '2791', '2760-2', '2760-2-a', '2760-2-b')
        )
        and etat_technique_rubrique = 'Exploité'
        and etat_administratif_rubrique = 'En vigueur'
        and libelle_etat_site = 'Avec titre'
    group by
        1,
        2
),

wastes as (
    select
        numero_identification_declarant as siret,
        date_reception,
        code_traitement,
        sum(quantite)                   as quantite
    from {{ ref('dnd_entrant') }}
    where
        unite = 'T'
        and date_reception >= '2022-01-01'
        and numero_identification_declarant in (
            select siret
            from
                installations
        )
    group by 1, 2, 3
),

wastes_rubriques as (
    select
        wastes.siret,
        wastes.date_reception as "day_of_processing",
        mrco.rubrique,
        sum(quantite)         as quantite_traitee
    from
        wastes
    left join {{ ref('referentiel_codes_operation_rubriques') }} as mrco
        on
            wastes.code_traitement = mrco.code_operation
    group by
        wastes.siret,
        wastes.date_reception,
        mrco.rubrique

)

select
    installations.*,
    wr.day_of_processing,
    wr.quantite_traitee
from
    installations
inner join wastes_rubriques as wr on
    installations.siret = wr.siret and installations.rubrique = wr.rubrique
