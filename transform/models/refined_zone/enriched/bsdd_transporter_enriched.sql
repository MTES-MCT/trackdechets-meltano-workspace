{{
  config(
    materialized = 'incremental',
    unique_key='id',
    indexes = [ 
        {'columns': ['id'] , 'unique': True },
        {'columns': ['created_at'] },
        {'columns': ['updated_at'] },
        { 'columns': ['emitter_company_siret'] },
        { 'columns' :['recipient_company_siret'] },
        { 'columns' :['transporter_company_siret'] },
        { 'columns' :['eco_organisme_siret'] },
        { 'columns' :['waste_details_code'] },
        { 'columns' :['waste_details_is_dangerous']},
        { 'columns' :['waste_details_pop']},
        { 'columns' :['form_id']}
    ]
    )
}}

with filtered_data as (
    select *
    from {{ ref('bsdd_transporter') }} as bt
    {% if is_incremental() %}
        where bt.updated_at >= (select max(updated_at) from {{ this }})
    {% endif %}
)

select
    bt.*,
    b.created_at as bordereau_created_at,
    b.emitter_company_siret,
    b.recipient_company_siret,
    b.eco_organisme_siret,
    b.waste_details_code,
    b.waste_details_is_dangerous,
    b.waste_details_pop,
    b.quantity_received,
    b.quantity_refused,
    b.processing_operation_done
from filtered_data as bt
left join {{ ref('bsdd') }} as b on bt.form_id = b.id
