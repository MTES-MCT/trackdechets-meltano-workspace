{{
  config(
    materialized = 'table',
    indexes=[{"columns":["siret","date_reception","rubrique"], "unique":True}]
    )
}}

with installations as (
    select
        siret,
        rubrique,
        max(raison_sociale)           as raison_sociale,
        array_agg(distinct code_aiot) as codes_aiot,
        sum(quantite_totale)          as quantite_autorisee
    from
        {{ ref('installations_rubriques_2024') }}
    where
        siret is not null
        and rubrique in ('2771', '2791', '2760-2')
    group by
        siret,
        rubrique
),

wastes as (
    select
        numero_identification_declarant as siret,
        date_reception,
        code_traitement,
        sum(quantite) as quantite
    from {{ ref('dnd_entrants') }}
    where unite = 'T'
    and date_reception >= '2022-01-01'
    and numero_identification_declarant in (
        select
            siret
        from
            installations
    )
    group by 1,2,3
),

wastes_rubriques as (
    select
        wastes.siret,
        wastes.date_reception,
        mrco.rubrique,
        sum(quantite) as quantite
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

select *
from wastes_rubriques
where rubrique in ('2771', '2791', '2760-2')