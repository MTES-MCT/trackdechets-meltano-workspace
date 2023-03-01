{{
  config(
    materialized = 'table',
    indexes = [ {'columns': ['recipient_company_siret'] }]
    )
}}

{{  create_bordereaux_enriched_query('bsdd',True,False) }}