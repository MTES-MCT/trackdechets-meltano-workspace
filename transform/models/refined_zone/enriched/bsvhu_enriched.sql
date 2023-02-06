{{ config(
    materialized = 'table',
    indexes = [ {'columns': ['created_at'] },{ 'columns': ['emitter_company_siret'] },{ 'columns' :['destination_company_siret'] },{ 'columns' :['transporter_company_siret'] }],
    post_hook = 'DROP TABLE IF EXISTS refined_zone_enriched.bsvhu_enriched_temp'
) }}

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
    e.code_departement AS "destination_departement",
    e.code_region AS "destination_region"
FROM
    {{ ref('bsvhu_enriched_temp') }}
    b
    LEFT JOIN etabs e
    ON b.destination_company_siret = e.siret
