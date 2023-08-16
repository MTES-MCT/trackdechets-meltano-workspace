{{
  config(
    materialized = 'table',
    indexes = [{"columns":["siret"]}]
    )
}}


with installations as (
select
        siret,
        rubrique || coalesce('-' || alinea,
    '') as rubrique,
        max(raison_sociale) as raison_sociale,
        array_agg(distinct code_aiot) as codes_aiot,
        sum(quantite_totale) as quantite_autorisee
from
        refined_zone_icpe.installations_rubriques ir
where
    siret is not null
    and rubrique = '2718'
    and alinea = '1'
group by
        siret,
        rubrique || coalesce('-' || alinea,
    '')
),
incoming_wastes as (
select
        b.destination_company_siret as siret,
        date_trunc(
            'day',
            b.received_at
        ) as "day",
        sum(b.quantity_received) as quantity,
        'incoming' as "type"
from
        refined_zone_enriched.bordereaux_enriched b
where
        b.destination_company_siret in (
    select
        siret
    from
        installations
        )
    and b.received_at >= '2022-01-01'
    and (
        waste_code ~* '.*\*$'
        or waste_pop
        or waste_is_dangerous
        )
    and b.processing_operation in (
    select
        rcor.code_operation
    from
        trusted_zone_gsheet.referentiel_codes_operation_rubriques rcor
    where
        rcor.rubrique = '2718'
        )
group by
        b.destination_company_siret,
        date_trunc(
            'day',
            b.received_at
        )
),
outgoing_wastes as (
select
        b.emitter_company_siret as siret,
        date_trunc(
            'day',
            b.taken_over_at
        ) as "day",
        -sum(b.quantity_received) as quantity,
        'outgoing' as "type"
from
        refined_zone_enriched.bordereaux_enriched b
where
        b.emitter_company_siret in (
    select
        siret
    from
        installations
        )
    and b.taken_over_at >= '2022-01-01'
    and (
            waste_code ~* '.*\*$'
        or waste_pop
        or waste_is_dangerous
        )
group by
        b.emitter_company_siret,
        date_trunc(
            'day',
            b.taken_over_at
        )
), 
grouped_data as (
select
    *
from
    incoming_wastes
union all
select
    *
from
    outgoing_wastes),
summed_data as (
select
    siret,
    day,
    sum(quantity) as quantity
from
    grouped_data
group by
    siret,
    day)
select 
    siret,
    day,
    quantity,
    sum(quantity) over (partition by siret order by day asc) as stock
from summed_data
