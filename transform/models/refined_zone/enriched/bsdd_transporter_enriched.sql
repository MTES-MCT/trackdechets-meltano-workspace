{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key='id',
    indexes = [ 
        {'columns': ['id'] , 'unique': True },
        {'columns': ['created_at'] },
        {'columns': ['updated_at'] },
        {'columns': ['taken_over_at'] },
        {'columns': ['bordereau_updated_at'] },
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



{% if is_incremental() %}
    with filtered_data as ( -- new bsdd_transporters lines
        select
            bt.*,
            b.created_at as bordereau_created_at,
            b.updated_at as bordereau_updated_at,
            b.status,
            b.emitter_company_siret,
            b.emitter_type,
            b.recipient_company_siret,
            b.eco_organisme_siret,
            b.waste_details_code,
            b.waste_details_is_dangerous,
            b.waste_details_pop,
            b.waste_details_quantity,
            b.quantity_received,
            b.quantity_refused,
            b.processing_operation_done
        from trusted_zone_trackdechets.bsdd_transporter as bt
        left join trusted_zone_trackdechets.bsdd as b on bt.form_id = b.id
        where
            bt.updated_at
            >= (
                select max(updated_at)
                from refined_zone_enriched.bsdd_transporter_enriched
            )
    ),

    -- existing transporter_enriched_line with updated_data
    filtered_enriched_data as (
        select
            t.id,
            t.number,
            t.transporter_company_siret,
            t.transporter_company_name,
            t.transporter_company_address,
            t.transporter_company_contact,
            t.transporter_company_phone,
            t.transporter_company_mail,
            t.transporter_is_exempted_of_receipt,
            t.transporter_receipt,
            t.transporter_department,
            t.transporter_validity_limit,
            t.transporter_number_plate,
            t.transporter_transport_mode,
            t.ready_to_take_over,
            t.taken_over_at,
            t.taken_over_by,
            t.created_at,
            t.updated_at,
            t.form_id,
            t.previous_transporter_company_org_id,
            t.transporter_company_vat_number,
            t.transporter_custom_info,
            b.created_at as bordereau_created_at,
            b.updated_at as bordereau_updated_at,
            b.status,
            b.emitter_company_siret,
            b.emitter_type,
            b.recipient_company_siret,
            b.eco_organisme_siret,
            b.waste_details_code,
            b.waste_details_is_dangerous,
            b.waste_details_pop,
            b.waste_details_quantity,
            b.quantity_received,
            b.quantity_refused,
            b.processing_operation_done
        from refined_zone_enriched.bsdd_transporter_enriched as t
        inner join
            trusted_zone_trackdechets.bsdd as b
            on t.form_id = b.id and t.bordereau_updated_at < b.updated_at
    ),

    joined_data as (
        select *
        from filtered_enriched_data
        union all
        select *
        from filtered_data
    ),

    final_data as (
        select
            *,
            row_number()
                over (
                    partition by id
                    order by greatest(updated_at, bordereau_updated_at)
                )
            as rn
        from joined_data
    )

    select *
    from final_data
    where rn = 1

{% else %} --full refresh
select
    bt.*,
    b.created_at as bordereau_created_at,
    b.updated_at as bordereau_updated_at,
    b.status,
    b.emitter_company_siret,
    b.emitter_type,
    b.recipient_company_siret,
    b.eco_organisme_siret,
    b.waste_details_code,
    b.waste_details_is_dangerous,
    b.waste_details_pop,
    b.waste_details_quantity,
    b.quantity_received,
    b.quantity_refused,
    b.processing_operation_done
from {{ ref('bsdd_transporter') }} as bt
left join {{ ref('bsdd') }} as b on bt.form_id = b.id
{% endif %}
