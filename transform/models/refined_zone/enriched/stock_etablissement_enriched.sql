{{
  config(
    materialized = 'table',
    indexes = [ 
        {'columns': ['siret'], 'unique': True },
    ]
    )
}}


WITH etabs AS (
    SELECT
    cgc.*,
    se.siret,
    se.activite_principale_etablissement
FROM
    {{ ref('stock_etablissement') }}
    AS se
LEFT JOIN
    {{ ref('code_geo_communes') }}
    AS cgc
    ON se.code_commune_etablissement = cgc.code_commune and cgc.type_commune != 'COMD'
),
coords as (
    select
    code_commune_insee,
    avg(latitude) as latitude,
    avg(longitude) as longitude
from
    {{ ref('base_codes_postaux') }} bcp
group by
    code_commune_insee
)
select 
etabs.*,
coords.latitude,
coords.longitude
from etabs
left join coords ON coords.code_commune_insee = etabs.code_commune
