{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{% set non_final_operation_codes %}
    (
        'D9',
        'D13',
        'D14',
        'D15',
        'R12',
        'R13'
    )
{% endset %}

WITH bordereaux as (
    SELECT
        b.id as bordereau_id,
        max(bp.operation_date) as "bordereaux_processed_at",
        bool_and(bp.operation_date is not null) as is_processed
    FROM {{ ref('bsff') }} b
    full outer join {{ ref('bsff_packaging') }} bp on b.id=bsff_id
    WHERE
        (b.waste_code ~* '.*\*$')
        and not b.is_draft
        and not b.is_deleted
    group by b.id
    having max(bp.operation_date) < DATE_TRUNC('week',now())
),
contenants AS (
    SELECT
        bp.id as packaging_id,
        bp.operation_code as "operation_code",
        case
            when bp.acceptation_weight > 60 then bp.acceptation_weight / 1000
            else bp.acceptation_weight
        END as "quantity",
        bp.operation_date
    FROM {{ ref('bsff') }} b
    LEFT JOIN {{ ref('bsff_packaging') }} bp ON b.id = bp.bsff_id
    WHERE
        operation_date < DATE_TRUNC('week',now())
        AND (b.waste_code ~* '.*\*$' or coalesce(bp.acceptation_waste_code ~* '.*\*$',false))
        and not is_draft
        and not is_deleted
)
SELECT
    DATE_TRUNC(
        'week',
        coalesce(c.operation_date,b.bordereaux_processed_at)
    ) AS "semaine",
    count(distinct bordereau_id) filter (where is_processed) as traitements_bordereaux,
    count(distinct packaging_id) as traitements_contenants,
    sum("quantity") as quantite_traitee,
    COUNT(distinct packaging_id) FILTER (WHERE operation_code in {{ non_final_operation_codes }}) AS traitements_contenants_operations_non_finales, 
    sum(quantity) FILTER (WHERE operation_code in {{ non_final_operation_codes }}) AS quantite_traitee_operations_non_finales,   
    COUNT(distinct packaging_id) FILTER (WHERE operation_code not in {{ non_final_operation_codes }}) AS traitements_contenants_operations_finales,
    sum(quantity) FILTER (WHERE operation_code not in {{ non_final_operation_codes }}) AS quantite_traitee_operations_finales
FROM
    contenants c
    left outer join bordereaux b on c.operation_date = b.bordereaux_processed_at
GROUP BY
    DATE_TRUNC(
        'week',
        coalesce(c.operation_date,b.bordereaux_processed_at)
    )
ORDER BY
    DATE_TRUNC(
        'week',
        coalesce(c.operation_date,b.bordereaux_processed_at)
    )
