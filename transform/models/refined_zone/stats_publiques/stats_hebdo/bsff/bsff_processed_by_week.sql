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
            b.destination_operation_code as "operation_code",
            b.destination_operation_signature_date
        FROM {{ ref('bsff') }} b
        WHERE
            destination_operation_signature_date < DATE_TRUNC('week',now())
            AND (b.waste_code ~* '.*\*$')
            and not is_draft
            and not is_deleted
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
            operation_date
        ) AS "semaine",
        count(distinct packaging_id) as traitements_contenants,
        sum("quantity") as quantite_traitee,
        COUNT(distinct packaging_id) FILTER (WHERE operation_code in {{ non_final_operation_codes }}) AS traitements_contenants_operations_non_finales, 
        sum(quantity) FILTER (WHERE operation_code in {{ non_final_operation_codes }}) AS quantite_traitee_operations_non_finales,   
        COUNT(distinct packaging_id) FILTER (WHERE operation_code not in {{ non_final_operation_codes }}) AS traitements_contenants_operations_finales,
        sum(quantity) FILTER (WHERE operation_code not in {{ non_final_operation_codes }}) AS quantite_traitee_operations_finales
    FROM
        contenants
    GROUP BY
        DATE_TRUNC(
            'week',
            operation_date
        )
    ORDER BY
        DATE_TRUNC(
            'week',
            operation_date
        )
    