{{ config(
    materialized = 'table',
    indexes = [ 
        {'columns': ['id'], 'unique': True },
        {'columns': ['created_at'] },
        {'columns': ['updated_at'] },
        {'columns': ['processed_at'] },
        { 'columns': ['emitter_company_siret'] },
        { 'columns' :['recipient_company_siret'] },
        { 'columns' :['transporter_company_siret'] },
        { 'columns' :['waste_details_code'] }
    ],
    post_hook=after_commit('DROP TABLE IF EXISTS refined_zone_enriched.bsdd_enriched_temp')
) }}

{{ create_bordereaux_enriched_query('bsdd_enriched_temp',False,True) }}
