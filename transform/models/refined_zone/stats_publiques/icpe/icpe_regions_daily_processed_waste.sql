{{
  config(
    materialized = 'table',
    indexes=[{"columns":["code_region_insee"]},{"columns":["rubrique"]}]
    )
}}

with stats as (
    select
        code_region_insee,
        rubrique,
        sum(quantite_autorisee) as quantite_autorisee
    from
        {{ ref('icpe_departements_daily_processed_waste') }} as ii
    group by
        ii.code_region_insee,
        rubrique
),

waste_processed_grouped as (
    select
        code_region_insee,
        rubrique,
        day_of_processing,
        sum(quantite_traitee) as quantite_traitee
    from
        {{ ref('icpe_departements_daily_processed_waste') }}
    group by
        code_region_insee,
        rubrique,
        day_of_processing
),

waste_stats as (
    select
        w.*,
        s.quantite_autorisee
    from
        waste_processed_grouped as w
    left join stats as s
        on
            w.code_region_insee = s.code_region_insee
            and w.rubrique = s.rubrique
    order by code_region_insee, rubrique, day_of_processing
)

select
    w.code_region_insee,
    cg.libelle as nom_departement,
    w.rubrique,
    w.day_of_processing,
    w.quantite_traitee,
    w.quantite_autorisee
from waste_stats as w
left join
    {{ ref('code_geo_regions') }} as cg
    on w.code_region_insee = cg.code_region
