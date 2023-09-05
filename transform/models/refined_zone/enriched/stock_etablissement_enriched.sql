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
        ON
            se.code_commune_etablissement = cgc.code_commune
            AND cgc.type_commune != 'COMD'
),

coords AS (
    SELECT
        code_commune_insee,
        avg(latitude)  AS latitude,
        avg(longitude) AS longitude
    FROM
        {{ ref('base_codes_postaux') }}
    GROUP BY
        code_commune_insee
)

SELECT
    etabs.*,
    coords.latitude,
    coords.longitude
FROM etabs
LEFT JOIN coords ON etabs.code_commune = coords.code_commune_insee
