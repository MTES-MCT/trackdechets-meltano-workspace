with quantities as (
    select
        *,
        case
            when code_operation_traitement like 'R%' then 'Déchet valorisé'
            when code_operation_traitement like 'D%' then 'Déchet éliminé'
            else 'Autre'
        end as operation_type
    from {{ ref('annual_quantity_processed_by_processing_operation') }}
)

select
    annee::int,
    operation_type as type_operation_traitement,
    sum(quantite_traitee) as quantite_traitee
from quantities
group by annee, operation_type
order by annee desc, operation_type