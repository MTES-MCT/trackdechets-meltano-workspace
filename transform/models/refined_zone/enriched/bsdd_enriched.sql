{{ config(
    materialized = 'table',
    indexes = [ {'columns': ['id'] },{'columns': ['created_at'] },
    { 'columns': ['emitter_company_siret'] },
    { 'columns' :['recipient_company_siret'] },
    { 'columns' :['transporter_company_siret'] },
    
    ],
    post_hook=after_commit('DROP TABLE IF EXISTS refined_zone_enriched.bsdd_enriched_temp')
) }}

{{  create_bordereaux_enriched_query('bsdd_enriched_temp',False,True) }}