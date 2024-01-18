{{
  config(
    materialized = 'table',
    indexes = [ 
        {'columns': ['id'] , 'unique': True },
        {'columns': ['created_at'] },
        {'columns': ['updated_at'] },
        { 'columns': ['emitter_company_siret'] },
        { 'columns' :['recipient_company_siret'] },
        { 'columns' :['transporter_company_siret'], },
        { 'columns' :['waste_details_code'] },
        { 'columns' :['waste_details_is_dangerous']},
        { 'columns' :['waste_details_pop']}
    ]
    )
}}


select
    bt.*,
    b.created_at as bordereau_created_at,
    b.emitter_company_siret,
    b.recipient_company_siret,
    b.waste_details_code,
    b.waste_details_is_dangerous,
    b.waste_details_pop,
    b.quantity_received
from {{ ref('bsdd_transporter') }} as bt
left join {{ ref('bsdd') }} as b on bt.form_id = b.id
