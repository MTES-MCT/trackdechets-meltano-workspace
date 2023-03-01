{{
  config(
    materialized = 'table',
    indexes = [ {'columns': ['destination_company_siret'] }]
    )
}}
{{  create_bordereaux_enriched_query('bsff',True,False) }}
