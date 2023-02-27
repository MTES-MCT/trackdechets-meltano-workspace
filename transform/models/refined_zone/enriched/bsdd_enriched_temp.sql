{{
  config(
    materialized = 'table',
    indexes = [ {'columns': ['recipient_company_siret'] }]
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
    INNER JOIN
        {{ ref('code_geo_communes') }}
        AS cgc
        ON se.code_commune_etablissement = cgc.code_commune
    WHERE
        cgc.type_commune = 'COM'
)

SELECT
    b.*,
    etabs.code_departement                  AS "emitter_departement",
    etabs.code_region                       AS "emitter_region",
    etabs.activite_principale_etablissement AS "emitter_naf"
FROM
    {{ ref('bsdd') }}
    AS b
LEFT JOIN etabs
    ON b.emitter_company_siret = etabs.siret
