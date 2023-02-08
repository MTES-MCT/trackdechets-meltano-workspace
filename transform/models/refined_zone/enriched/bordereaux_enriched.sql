select
    'BSDD' as "_bs_type",
    id,
    "created_at",
    "taken_over_at",
    "received_at",
    "processed_at",
    status,
    quantity_received,
    "processing_operation_done" as "processing_operation",
    "waste_details_code" as "waste_code",
    "waste_details_pop" as "waste_pop",
    emitter_company_siret,
    emitter_company_name,
    emitter_departement,
    emitter_region,
    transporter_company_siret,
    transporter_company_name,
    recipient_company_siret as "destination_company_siret",
    recipient_departement as "destination_departement",
    recipient_region as "destination_region",
    recipient_company_name as "destination_company_name"
from
    {{ ref('bsdd_enriched') }}
union all
select
    'BSDA' as "_bs_type",
    id,
    "created_at",
    transporter_transport_taken_over_at as "taken_over_at",
    destination_reception_date as "received_at",
    destination_operation_date as "processed_at",
    status,
    "destination_reception_weight" as "quantity_received",
    "destination_operation_code" as "processing_operation",
    "waste_code",
    "waste_pop",
    emitter_company_siret,
    emitter_company_name,
    emitter_departement,
    emitter_region,
    transporter_company_siret,
    transporter_company_name,
    destination_company_siret,
    destination_departement,
    destination_region,
    destination_company_name
from
    {{ ref('bsda_enriched') }}
union all
select
    'BSFF' as "_bs_type",
    id,
    "created_at",
    transporter_transport_taken_over_at as "taken_over_at",
    destination_reception_date as "received_at",
    destination_operation_signature_date as "processed_at",
    status,
    "destination_reception_weight" as "quantity_received",
    "destination_operation_code" as "processing_operation",
    "waste_code",
    null as "waste_pop",
    emitter_company_siret,
    emitter_company_name,
    emitter_departement,
    emitter_region,
    transporter_company_siret,
    transporter_company_name,
    destination_company_siret,
    destination_departement,
    destination_region,
    destination_company_name
from
    {{ ref('bsff_enriched') }}
union all
select
    'BSDASRI' as "_bs_type",
    id,
    "created_at",
    transporter_taken_over_at as "taken_over_at",
    destination_reception_date as "received_at",
    destination_operation_date as "processed_at",
    status,
    "destination_reception_waste_weight_value" as "quantity_received",
    "destination_operation_code" as "processing_operation",
    "waste_code",
    null as "waste_pop",
    emitter_company_siret,
    emitter_company_name,
    emitter_departement,
    emitter_region,
    transporter_company_siret,
    transporter_company_name,
    destination_company_siret,
    destination_departement,
    destination_region,
    destination_company_name
from
    {{ ref('bsdasri_enriched') }}
union all
select
    'BSVHU' as "_bs_type",
    id,
    "created_at",
    transporter_transport_taken_over_at as "taken_over_at",
    destination_reception_date as "received_at",
    destination_operation_date as "processed_at",
    status,
    "destination_reception_weight" as "quantity_received",
    "destination_operation_code" as "processing_operation",
    "waste_code",
    null as "waste_pop",
    emitter_company_siret,
    emitter_company_name,
    emitter_departement,
    emitter_region,
    transporter_company_siret,
    transporter_company_name,
    destination_company_siret,
    destination_departement,
    destination_region,
    destination_company_name
from
    {{ ref('bsvhu_enriched') }}
