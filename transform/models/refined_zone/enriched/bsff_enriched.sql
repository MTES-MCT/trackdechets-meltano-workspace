{{ config(
    materialized = 'table',
    indexes = [ 
        {'columns': ['id'] , 'unique': True },
        {'columns': ['created_at'] },
        { 'columns': ['emitter_company_siret'] },
        { 'columns' :['destination_company_siret'] },
        { 'columns' :['transporter_company_siret'], },
        { 'columns' :['waste_code'] }
    ],
    post_hook = after_commit('DROP TABLE IF EXISTS refined_zone_enriched.bsff_enriched_temp')
) }}

{{ create_bordereaux_enriched_query('bsff_enriched_temp',False,False) }}
