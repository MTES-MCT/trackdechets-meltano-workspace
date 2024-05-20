{{ config(
  pre_hook = "{{ create_indexes_for_source([
    'readableId',
    'createdAt',
    'updatedAt',
    'emitterCompanySiret',
    'currentTransporterOrgId',
    'recipientCompanySiret',
    'wasteDetailsCode',
    'wasteDetailsPop',
    'wasteDetailsIsDangerous',
    'transportersSirets',
    'sentAt',
    'receivedAt',
    'processedAt']) }}"
) }}

with source as (
    select * from {{ source('raw_zone_trackdechets', 'bsdd_raw') }}
),

renamed as (
    select
        id,
        "createdAt"                         as created_at,
        "updatedAt"                         as updated_at,
        "emitterType"                       as emitter_type,
        "emitterPickupSite"                 as emitter_pickup_site,
        "emitterCompanyName"                as emitter_company_name,
        "emitterCompanySiret"               as emitter_company_siret,
        "emitterCompanyAddress"             as emitter_company_address,
        "emitterCompanyContact"             as emitter_company_contact,
        "emitterCompanyPhone"               as emitter_company_phone,
        "emitterCompanyMail"                as emitter_company_mail,
        "recipientCap"                      as recipient_cap,
        "recipientCompanyName"              as recipient_company_name,
        "recipientCompanySiret"             as recipient_company_siret,
        "recipientCompanyAddress"           as recipient_company_address,
        "recipientCompanyContact"           as recipient_company_contact,
        "recipientCompanyPhone"             as recipient_company_phone,
        "recipientCompanyMail"              as recipient_company_mail,
        "wasteDetailsCode"                  as waste_details_code,
        "wasteDetailsOnuCode"               as waste_details_onu_code,
        "wasteDetailsQuantity"              as waste_details_quantity,
        "wasteDetailsQuantityType"          as waste_details_quantity_type,
        "readableId"                        as readable_id,
        status,
        cast("sentAt" as timestamptz)       as sent_at,
        "sentBy"                            as sent_by,
        "isAccepted"                        as is_accepted,
        cast("receivedAt" as timestamptz)   as received_at,
        "quantityReceived"                  as quantity_received,
        "wasteDetailsName"                  as waste_details_name,
        "isDeleted"                         as is_deleted,
        "receivedBy"                        as received_by,
        "wasteDetailsConsistence"           as waste_details_consistence,
        "processedBy"                       as processed_by,
        cast("processedAt" as timestamptz)  as processed_at,
        "traderCompanyName"                 as trader_company_name,
        "traderCompanySiret"                as trader_company_siret,
        "traderCompanyAddress"              as trader_company_address,
        "traderCompanyContact"              as trader_company_contact,
        "traderCompanyPhone"                as trader_company_phone,
        "traderCompanyMail"                 as trader_company_mail,
        "traderReceipt"                     as trader_receipt,
        "traderDepartment"                  as trader_department,
        "traderValidityLimit"               as trader_validity_limit,
        "processingOperationDescription"    as processing_operation_description,
        "noTraceability"                    as no_traceability,
        "signedByTransporter"               as signed_by_transporter,
        "customId"                          as custom_id,
        "wasteAcceptationStatus"            as waste_acceptation_status,
        "wasteRefusalReason"                as waste_refusal_reason,
        "nextDestinationCompanyName"        as next_destination_company_name,
        "nextDestinationCompanySiret"       as next_destination_company_siret,
        "nextDestinationCompanyAddress"     as next_destination_company_address,
        "nextDestinationCompanyContact"     as next_destination_company_contact,
        "nextDestinationCompanyPhone"       as next_destination_company_phone,
        "nextDestinationCompanyMail"        as next_destination_company_mail,
        "emitterWorkSiteName"               as emitter_work_site_name,
        "emitterWorkSiteAddress"            as emitter_work_site_address,
        "emitterWorkSiteCity"               as emitter_work_site_city,
        "emitterWorkSitePostalCode"         as emitter_work_site_postal_code,
        "emitterWorkSiteInfos"              as emitter_work_site_infos,
        "recipientIsTempStorage"            as recipient_is_temp_storage,
        cast("signedAt" as timestamptz)     as signed_at,
        "currentTransporterOrgId"           as current_transporter_org_id,
        "nextTransporterOrgId"              as next_transporter_org_id,
        "nextDestinationCompanyCountry"     as next_destination_company_country,
        "isImportedFromPaper"               as is_imported_from_paper,
        "ecoOrganismeName"                  as eco_organisme_name,
        "ecoOrganismeSiret"                 as eco_organisme_siret,
        "wasteDetailsPackagingInfos"        as waste_details_packaging_infos,
        "signedBy"                          as signed_by,
        "wasteDetailsPop"                   as waste_details_pop,
        "ownerId"                           as owner_id,
        "brokerCompanyName"                 as broker_company_name,
        "brokerCompanySiret"                as broker_company_siret,
        "brokerCompanyAddress"              as broker_company_address,
        "brokerCompanyContact"              as broker_company_contact,
        "brokerCompanyPhone"                as broker_company_phone,
        "brokerCompanyMail"                 as broker_company_mail,
        "brokerReceipt"                     as broker_receipt,
        "brokerDepartment"                  as broker_department,
        "brokerValidityLimit"               as broker_validity_limit,
        "wasteDetailsIsDangerous"           as waste_details_is_dangerous,
        cast("emittedAt" as timestamptz)    as emitted_at,
        "emittedBy"                         as emitted_by,
        "emittedByEcoOrganisme"             as emitted_by_eco_organisme,
        cast("takenOverAt" as timestamptz)  as taken_over_at,
        "takenOverBy"                       as taken_over_by,
        "wasteDetailsParcelNumbers"         as waste_details_parcel_numbers,
        "wasteDetailsAnalysisReferences"    as waste_details_analysis_references,
        "wasteDetailsLandIdentifiers"       as waste_details_land_identifiers,
        "forwardedInId"                     as forwarded_in_id,
        "quantityReceivedType"              as quantity_received_type,
        "emitterIsForeignShip"              as emitter_is_foreign_ship,
        "emitterIsPrivateIndividual"        as emitter_is_private_individual,
        "emitterCompanyOmiNumber"           as emitter_company_omi_number,
        "nextDestinationCompanyVatNumber"   as next_destination_company_vat_number,
        "recipientsSirets"                  as recipients_sirets,
        "transportersSirets"                as transporters_sirets,
        "intermediariesSirets"              as intermediaries_sirets,
        "nextDestinationNotificationNumber" as next_destination_notification_number,
        "wasteDetailsSampleNumber"          as waste_details_sample_number,
        "canAccessDraftSirets"              as can_access_draft_sirets,
        "destinationOperationMode"          as destination_operation_mode,
        "quantityGrouped"                   as quantity_grouped,
        replace(
            "recipientProcessingOperation", ' ', ''
        )                                   as recipient_processing_operation,
        replace(
            "processingOperationDone", ' ', ''
        )                                   as processing_operation_done,
        replace(
            "nextDestinationProcessingOperation", ' ', ''
        )                                   as next_destination_processing_operation
    from
        source
)

select
    id,
    created_at,
    updated_at,
    emitter_type,
    emitter_pickup_site,
    emitter_company_name,
    emitter_company_siret,
    emitter_company_address,
    emitter_company_contact,
    emitter_company_phone,
    emitter_company_mail,
    recipient_cap,
    recipient_processing_operation,
    recipient_company_name,
    recipient_company_siret,
    recipient_company_address,
    recipient_company_contact,
    recipient_company_phone,
    recipient_company_mail,
    waste_details_code,
    waste_details_onu_code,
    waste_details_quantity,
    waste_details_quantity_type,
    readable_id,
    status,
    sent_at,
    sent_by,
    is_accepted,
    received_at,
    quantity_received,
    processing_operation_done,
    waste_details_name,
    is_deleted,
    received_by,
    waste_details_consistence,
    processed_by,
    processed_at,
    next_destination_processing_operation,
    trader_company_name,
    trader_company_siret,
    trader_company_address,
    trader_company_contact,
    trader_company_phone,
    trader_company_mail,
    trader_receipt,
    trader_department,
    trader_validity_limit,
    processing_operation_description,
    no_traceability,
    signed_by_transporter,
    custom_id,
    waste_acceptation_status,
    waste_refusal_reason,
    next_destination_company_name,
    next_destination_company_siret,
    next_destination_company_address,
    next_destination_company_contact,
    next_destination_company_phone,
    next_destination_company_mail,
    emitter_work_site_name,
    emitter_work_site_address,
    emitter_work_site_city,
    emitter_work_site_postal_code,
    emitter_work_site_infos,
    recipient_is_temp_storage,
    signed_at,
    current_transporter_org_id,
    next_transporter_org_id,
    next_destination_company_country,
    is_imported_from_paper,
    eco_organisme_name,
    eco_organisme_siret,
    waste_details_packaging_infos,
    signed_by,
    waste_details_pop,
    owner_id,
    broker_company_name,
    broker_company_siret,
    broker_company_address,
    broker_company_contact,
    broker_company_phone,
    broker_company_mail,
    broker_receipt,
    broker_department,
    broker_validity_limit,
    waste_details_is_dangerous,
    emitted_at,
    emitted_by,
    emitted_by_eco_organisme,
    taken_over_at,
    taken_over_by,
    waste_details_parcel_numbers,
    waste_details_analysis_references,
    waste_details_land_identifiers,
    forwarded_in_id,
    quantity_received_type,
    emitter_is_foreign_ship,
    emitter_is_private_individual,
    emitter_company_omi_number,
    next_destination_company_vat_number,
    recipients_sirets,
    transporters_sirets,
    intermediaries_sirets,
    next_destination_notification_number,
    waste_details_sample_number,
    can_access_draft_sirets,
    destination_operation_mode,
    quantity_grouped
from renamed
