{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    on_schema_change='append_new_columns',
    indexes = [ 
        {'columns': ['id'] , 'unique': True },
        {'columns': ['created_at'] },
        {'columns': ['updated_at'] },
        {'columns': ['destination_operation_date'] },
        { 'columns': ['emitter_company_siret'] },
        { 'columns' :['destination_company_siret'] },
        { 'columns' :['transporter_company_siret'] },
        { 'columns' :['waste_code'] }
        ]
) }}

{{ create_bordereaux_enriched_query('bsvhu',False) }}
