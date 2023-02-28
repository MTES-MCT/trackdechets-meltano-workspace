{{
  config(
    materialized = 'table',
    indexes = [ {'columns': ['destination_company_siret'] }]
    )
}}
WITH etabs AS (
    {{ create_sirene_etabs_enriched_statement() }}
)
SELECT
    b.*,
    etabs.code_departement                  AS "emitter_departement",
    etabs.code_region                       AS "emitter_region",
    etabs.activite_principale_etablissement AS "emitter_naf"
FROM
    {{ ref('bsff') }}
    AS b
LEFT JOIN etabs
    ON b.emitter_company_siret = etabs.siret
