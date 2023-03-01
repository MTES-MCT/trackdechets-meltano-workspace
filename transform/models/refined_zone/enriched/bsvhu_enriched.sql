{{ config(
    materialized = 'table',
    indexes = [ {'columns': ['id'] },{'columns': ['created_at'] },{ 'columns': ['emitter_company_siret'] },{ 'columns' :['destination_company_siret'] },{ 'columns' :['transporter_company_siret'] }],
    post_hook = after_commit('DROP TABLE IF EXISTS refined_zone_enriched.bsvhu_enriched_temp')
) }}

{{  create_bordereaux_enriched_query('bsvhu_enriched_temp',False,False) }}