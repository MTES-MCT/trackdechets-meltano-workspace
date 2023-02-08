{{ config(
    materialized = 'table',
    indexes = [ {'columns': ['created_at'] },
    { 'columns': ['emitter_company_siret'] },
    { 'columns' :['recipient_company_siret'] },
    { 'columns' :['transporter_company_siret'] }],
    post_hook='DROP TABLE IF EXISTS refined_zone_enriched.bsdd_enriched_temp'
) }}

WITH etabs AS (

    SELECT
        cgc.*,
        se.siret
    FROM
        {{ ref('stock_etablissement') }}
        AS se
    INNER JOIN
        {{ ref('code_geo_communes') }}
        AS cgc
        ON se.code_commune_etablissement = cgc.code_commune
    WHERE
        cgc.type_commune = 'COM'
)

SELECT
    b.*,
    etabs.code_departement AS "recipient_departement",
    etabs.code_region AS "recipient_region"
FROM
    {{ ref('bsdd_enriched_temp') }}
    AS b
LEFT JOIN etabs
    ON b.recipient_company_siret = etabs.siret
