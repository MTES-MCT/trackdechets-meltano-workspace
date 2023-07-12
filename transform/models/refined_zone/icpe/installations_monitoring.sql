{{
  config(
    materialized = 'table',
    )
}}

with installations as (
select
    siret,
    rubrique || coalesce('-' || alinea,'') as rubrique,
    max(raison_sociale) as raison_sociale,
    array_agg(distinct code_aiot) as codes_aiot,
    sum(quantite_totale) as quantite_autorisee
from
    {{ ref('installations_rubriques') }}
where
    siret is not null
group by
    siret, rubrique || coalesce('-' || alinea,'')
    ),
wastes as (
select
    b.destination_company_siret as siret,
    extract( year
from
    b.processed_at) as "year",
    b.processing_operation,
    sum(b.quantity_received) as quantite_traitee,
    sum(b.quantity_received)/365 as quantite_moyenne_j
from
    {{ ref('bordereaux_enriched') }} b
where
    b.destination_company_siret in (
    select
        siret
    from
        installations)
    and b.processed_at >= '2022-01-01'
group by
    b.destination_company_siret ,
    extract(year
from
    b.processed_at),
    b.processing_operation  
),
wastes_rubriques as (
select
    wastes.*,
    mrco.rubrique
from
    wastes
left join trusted_zone.mapping_rubrique_code_operation mrco on
    wastes.processing_operation = mrco.code_operation 
)
select
    installations.*,
    wr."year",
    wr.quantite_traitee,
    wr.quantite_moyenne_j
from
    installations
left join wastes_rubriques wr on
    installations.siret = wr.siret and installations.rubrique = wr.rubrique