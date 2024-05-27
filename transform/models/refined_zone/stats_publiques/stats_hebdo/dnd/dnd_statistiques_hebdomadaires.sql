{{
  config(
    materialized = 'table',
    )
}}

with entrants as (
    select
        date_trunc(
            'week', date_reception
        )                               as semaine,
        count(
            distinct id
        )                               as nombre_declarations_dnd_entrants,
        sum(case
            when quantite > 60
                then quantite / 1000
            else quantite
        end) filter (where unite = 'T') as quantite_dnd_entrants,
        sum(quantite) filter (
            where unite = 'M3'
        )                               as volume_dnd_entrants
    from {{ ref("dnd_entrants") }}
    where date_trunc('week', date_reception) < date_trunc('week', now())
    group by 1
),

sortants as (
    select
        date_trunc(
            'week', date_expedition
        )                               as semaine,
        count(
            distinct id
        )                               as nombre_declarations_dnd_sortants,
        sum(case
            when quantite > 60
                then quantite / 1000
            else quantite
        end) filter (where unite = 'T') as quantite_dnd_sortants,
        sum(quantite) filter (
            where unite = 'M3'
        )                               as volume_dnd_sortants
    from {{ ref("dnd_sortants") }}
    where date_trunc('week', date_expedition) < date_trunc('week', now())
    group by 1
)

select
    coalesce(entrants.semaine, sortants.semaine) as semaine,
    coalesce(
        entrants.nombre_declarations_dnd_entrants, 0
    )                                            as nombre_declarations_dnd_entrants,
    coalesce(
        sortants.nombre_declarations_dnd_sortants, 0
    )                                            as nombre_declarations_dnd_sortants,
    coalesce(
        entrants.quantite_dnd_entrants, 0
    )                                            as quantite_dnd_entrants,
    coalesce(entrants.volume_dnd_entrants, 0)    as volume_dnd_entrants,
    coalesce(
        sortants.quantite_dnd_sortants, 0
    )                                            as quantite_dnd_sortants,
    coalesce(sortants.volume_dnd_sortants)       as volume_dnd_sortants
from entrants
full outer join sortants on entrants.semaine = sortants.semaine
order by 1 desc
