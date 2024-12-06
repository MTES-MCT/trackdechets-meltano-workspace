{{
  config(
    materialized = 'table',
    )
}}

with td_data as (
    select
        'donnees_td'                    as origine_donnee,
        date_part('year', processed_at) as annee,
        waste_code                      as code_dechet,
        'tonne'                         as unite,
        sum(quantity_received)          as quantite_traitee
    from {{ ref('bordereaux_enriched') }}
    where
        processing_operation not in
        (
            'D9',
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
        and waste_code in (
            '20 01 35*',
            '20 01 36',
            '16 02 09*',
            '16 02 10*',
            '16 02 11*',
            '16 02 13*',
            '16 02 14',
            '16 02 15*',
            '16 02 16'
        )
        and date_part('year', processed_at) in (2023, 2024)
    group by 2, 3
    order by 2, 3
),

rndts_data as (
    select
        'donnees_rndts'                      as origine_donnee,
        date_part('year', de.date_reception) as annee,
        de.code_dechet,
        case de.code_unite
            when 'T' then 'tonne'
            when 'm³' then de.code_unite
        end
        as unite,
        sum(de.quantite)                     as quantite_traitee
    from {{ ref('dnd_entrant') }} as de
    where
        de.code_traitement not in
        (
            'D9',
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
        and de.code_dechet in (
            '20 01 35*',
            '20 01 36',
            '16 02 09*',
            '16 02 10*',
            '16 02 11*',
            '16 02 13*',
            '16 02 14',
            '16 02 15*',
            '16 02 16'
        )
        and date_part('year', de.date_reception) in (2023, 2024)
    group by 2, 3, 4
    order by 2, 3
)

select *
from td_data
union all
select *
from rndts_data
