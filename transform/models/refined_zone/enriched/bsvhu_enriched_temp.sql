{{
  config(
    materialized = 'table',
    indexes = [ {'columns': ['destination_company_siret'] }]
    )
}}
WITH etabs AS (
    SELECT
        se.siret,
        cgc.*
    FROM
        {{ ref('stock_etablissement') }}
        se
        INNER JOIN {{ ref('code_geo_communes') }}
        cgc
        ON se.code_commune_etablissement = cgc.code_commune
    WHERE
        cgc.type_commune = 'COM'
)
SELECT
    b.*,
    e.code_departement AS "emitter_departement",
    e.code_region AS "emitter_region"
FROM
    {{ ref('bsvhu') }}
    b
    LEFT JOIN etabs e
    ON b.emitter_company_siret = e.siret
