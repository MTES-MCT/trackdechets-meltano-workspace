select
    'BSDD'                            as _bs_type,
    id,
    readable_id,
    created_at,
    taken_over_at,
    received_at,
    processed_at,
    status,
    no_traceability,
    coalesce(status = 'DRAFT', false) as is_draft,
    quantity_received,
    null::numeric                     as accepted_quantity_packagings,
    processing_operation_done         as processing_operation,
    waste_details_code                as waste_code,
    waste_details_name                as waste_name,
    waste_details_pop                 as waste_pop,
    waste_details_is_dangerous        as waste_is_dangerous,
    emitter_company_siret,
    emitter_company_name,
    emitter_company_address,
    emitter_commune,
    emitter_departement,
    emitter_region,
    emitter_naf,
    null                              as worker_company_siret,
    null                              as worker_company_name,
    current_transporter_org_id        as transporter_company_siret,
    transporters_sirets,
    null                              as transporter_company_name,
    null                              as transport_mode,
    recipient_company_siret           as destination_company_siret,
    recipient_company_name            as destination_company_name,
    recipient_company_address         as destination_company_address,
    recipient_commune                 as destination_commune,
    recipient_departement             as destination_departement,
    recipient_region                  as destination_region,
    recipient_naf                     as destination_naf,
    eco_organisme_siret,
    eco_organisme_name
from
    {{ ref('bsdd_enriched') }}
where not is_deleted
union all
select
    'BSDA'                               as _bs_type,
    id,
    id                                   as readable_id,
    created_at,
    transporter_transport_signature_date as taken_over_at,
    destination_reception_date           as received_at,
    destination_operation_date           as processed_at,
    status,
    null                                 as no_traceability,
    is_draft,
    destination_reception_weight         as quantity_received,
    null::numeric                        as accepted_quantity_packagings,
    destination_operation_code           as processing_operation,
    waste_code,
    waste_material_name                  as waste_name,
    waste_pop,
    null                                 as waste_is_dangerous,
    emitter_company_siret,
    emitter_company_name,
    emitter_company_address,
    emitter_commune,
    emitter_departement,
    emitter_region,
    emitter_naf,
    worker_company_siret,
    worker_company_name,
    null                                 as transporter_company_siret,
    null                                 as transporters_sirets,
    null                                 as transporter_company_name,
    null                                 as transport_mode,
    destination_company_siret,
    destination_company_name,
    destination_company_address,
    destination_commune,
    destination_departement,
    destination_region,
    destination_naf,
    eco_organisme_siret,
    eco_organisme_name
from
    {{ ref('bsda_enriched') }}
where not is_deleted
union all
select
    'BSFF'                              as _bs_type,
    id,
    id                                  as readable_id,
    created_at,
    transporter_transport_taken_over_at as taken_over_at,
    destination_reception_date          as received_at,
    null                                as processed_at,
    status,
    null                                as no_traceability,
    is_draft,
    null                                as quantity_received,
    accepted_quantity_packagings,
    null                                as processing_operation,
    waste_code,
    waste_description                   as waste_name,
    null                                as waste_pop,
    null                                as waste_is_dangerous,
    emitter_company_siret,
    emitter_company_name,
    emitter_company_address,
    emitter_commune,
    emitter_departement,
    emitter_region,
    emitter_naf,
    null                                as worker_company_siret,
    null                                as worker_company_name,
    transporter_company_siret,
    null                                as transporters_sirets,
    transporter_company_name,
    transporter_transport_mode          as transport_mode,
    destination_company_siret,
    destination_company_name,
    destination_company_address,
    destination_commune,
    destination_departement,
    destination_region,
    destination_naf,
    null                                as eco_organisme_siret,
    null                                as eco_organisme_name
from
    {{ ref('bsff_enriched') }}
where not is_deleted
union all
select
    'BSDASRI'                                as _bs_type,
    id,
    id                                       as readable_id,
    created_at,
    transporter_taken_over_at                as taken_over_at,
    destination_reception_date               as received_at,
    destination_operation_date               as processed_at,
    status,
    null                                     as no_traceability,
    is_draft,
    destination_reception_waste_weight_value as quantity_received,
    null::numeric                            as accepted_quantity_packagings,
    destination_operation_code               as processing_operation,
    waste_code,
    null                                     as waste_name,
    null                                     as waste_pop,
    null                                     as waste_is_dangerous,
    emitter_company_siret,
    emitter_company_name,
    emitter_company_address,
    emitter_commune,
    emitter_departement,
    emitter_region,
    emitter_naf,
    null                                     as worker_company_siret,
    null                                     as worker_company_name,
    transporter_company_siret,
    null                                     as transporters_sirets,
    transporter_company_name,
    transporter_transport_mode               as transport_mode,
    destination_company_siret,
    destination_company_name,
    destination_company_address,
    destination_commune,
    destination_departement,
    destination_region,
    destination_naf,
    eco_organisme_siret,
    eco_organisme_name
from
    {{ ref('bsdasri_enriched') }}
where not is_deleted
union all
select
    'BSVHU'                             as _bs_type,
    id,
    id                                  as readable_id,
    created_at,
    transporter_transport_taken_over_at as taken_over_at,
    destination_reception_date          as received_at,
    destination_operation_date          as processed_at,
    status,
    null                                as no_traceability,
    is_draft,
    destination_reception_weight        as quantity_received,
    null::numeric                       as accepted_quantity_packagings,
    destination_operation_code          as processing_operation,
    waste_code,
    null                                as waste_name,
    null                                as waste_pop,
    null                                as waste_is_dangerous,
    emitter_company_siret,
    emitter_company_name,
    emitter_company_address,
    emitter_commune,
    emitter_departement,
    emitter_region,
    emitter_naf,
    null                                as worker_company_siret,
    null                                as worker_company_name,
    transporter_company_siret,
    null                                as transporters_sirets,
    transporter_company_name,
    null                                as transport_mode,
    destination_company_siret,
    destination_company_name,
    destination_company_address,
    destination_commune,
    destination_departement,
    destination_region,
    destination_naf,
    null                                as eco_organisme_siret,
    null                                as eco_organisme_name
from
    {{ ref('bsvhu_enriched') }}
where not is_deleted
