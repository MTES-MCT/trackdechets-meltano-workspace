SELECT
    id as "id",
    createdat as "created_at",
    updatedat as "updated_at",
    isdeleted as "is_deleted",
    isdraft as "is_draft",
    status as "status",
    wastecode as "waste_code",
    quantity as "quantity",
    weightvalue/1000 as "weight_value", -- Passage en tonnes
    weightisestimate as "weight_is_estimate",
    packaging as "packaging",
    emittercompanysiret as "emitter_company_siret",
    emittercompanyname as "emitter_company_name",
    emittercompanyaddress as "emitter_company_address",
    emittercompanycontact as "emitter_company_contact",
    emittercompanymail as "emitter_company_mail",
    emittercompanyphone as "emitter_company_phone",
    emittercustominfo as "emitter_custom_info",
    emitteragrementnumber as "emitter_agrement_number",
    emitteremissionsignaturedate as "emitter_emission_signature_date",
    emitteremissionsignatureauthor as "emitter_emission_signature_author",
    transportercompanysiret as "transporter_company_siret",
    transportercompanyname as "transporter_company_name",
    transportercompanyaddress as "transporter_company_address",
    transportercompanycontact as "transporter_company_contact",
    transportercompanymail as "transporter_company_mail",
    transportercompanyphone as "transporter_company_phone",
    transportercustominfo as "transporter_custom_info",
    transportercompanyvatnumber as "transporter_company_vat_number",
    transporterrecepissenumber as "transporter_recepisse_number",
    transporterrecepissedepartment as "transporter_recepisse_department",
    transporterrecepissevaliditylimit as "transporter_recepisse_validity_limit",
    transportertransportplates as "transporter_transport_plates",
    transportertransporttakenoverat as "transporter_transport_taken_over_at",
    transportertransportsignaturedate as "transporter_transport_signature_date",
    transportertransportsignatureauthor as "transporter_transport_signature_author",
    destinationcompanysiret as "destination_company_siret",
    destinationcompanyname as "destination_company_name",
    destinationcompanyaddress as "destination_company_address",
    destinationcompanycontact as "destination_company_contact",
    destinationcompanymail as "destination_company_mail",
    destinationcompanyphone as "destination_company_phone",
    destinationcustominfo as "destination_custom_info",
    destinationagrementnumber as "destination_agrement_number",
    destinationreceptiondate as "destination_reception_date",
    destinationreceptionweight/1000 as "destination_reception_weight", -- Passage en tonnes
    destinationreceptionquantity as "destination_reception_quantity",
    destinationreceptionidentificationnumbers as "destination_reception_identification_numbers",
    destinationreceptionidentificationtype as "destination_reception_identification_type",
    destinationreceptionacceptationstatus as "destination_reception_acceptation_status",
    destinationreceptionrefusalreason as "destination_reception_refusal_reason",
    destinationoperationcode as "destination_operation_code",
    destinationoperationdate as "destination_operation_date",
    destinationoperationsignaturedate as "destination_operation_signature_date",
    destinationoperationsignatureauthor as "destination_operation_signature_author",
    destinationplannedoperationcode as "destination_planned_operation_code",
    destinationtype as "destination_type",
    destinationoperationnextdestinationcompanysiret as "destination_operation_next_destination_company_siret",
    destinationoperationnextdestinationcompanyname as "destination_operation_next_destination_company_name",
    destinationoperationnextdestinationcompanyaddress as "destination_operation_next_destination_company_address",
    destinationoperationnextdestinationcompanycontact as "destination_operation_next_destination_company_contact",
    destinationoperationnextdestinationcompanymail as "destination_operation_next_destination_company_mail",
    destinationoperationnextdestinationcompanyphone as "destination_operation_next_destination_company_phone",
    destinationoperationnextdestinationcompanyvatnumber as "destination_operation_next_destination_company_vat_number",  
    identificationnumbers as "identification_numbers",
    identificationtype as "identification_type"
FROM
   {{ source("raw_zone_trackdechets", "bsvhu_raw") }}
