with quantities as (
    select
        *,
        case
            when processing_operation like 'R%' then 'Déchet valorisé'
            when processing_operation like 'D%' then 'Déchet éliminé'
            else 'Autre'
        end as operation_type
    from {{ ref('annual_quantity_processed_by_processing_operation') }}
)

select
    year_of_processing as annee_traitement,
    operation_type as type_operation,
    sum(quantity_processed) as quantite_traitee
from quantities
group by year_of_processing, operation_type
