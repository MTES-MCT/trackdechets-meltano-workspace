{{ config(
    materialized = 'table',
    indexes = [ 
        {'columns': ['id'] , 'unique': True },
        {'columns': ['created_at'] },
        {'columns': ['destination_operation_date'] },
        { 'columns': ['emitter_company_siret'] },
        { 'columns' :['destination_company_siret'] },
        { 'columns' :['transporter_company_siret'] },
        { 'columns' :['waste_code'] }
    ],
    post_hook = after_commit('DROP TABLE IF EXISTS refined_zone_enriched.bsda_enriched_temp')
) }}

{{ create_bordereaux_enriched_query('bsda_enriched_temp',False,False) }}
