{% macro create_bordereaux_counts(
        model_name,
        date_column_name,
        count_name,
        quantity_name
    ) -%}

    {% set non_final_processing_operation_codes %}
    (
        'D9',
        'D13',
        'D14',
        'D15',
        'R12',
        'R13'
    )
    {% endset %}

    {% set quantity_column_name %}
       {% if model_name == "bsdd" %}
            quantity_received
        {% elif model_name == "bsff_packaging"%}
            acceptation_weight
        {% elif model_name == "bsdasri"%}
            destination_reception_waste_weight_value
        {% else %}
            destination_reception_weight
        {% endif %}
    {% endset %}

    {% set processing_operation_column_name %}
        {% if model_name == "bsdd" %}
            processing_operation_done
        {% elif model_name == "bsff_packaging"%}
            operation_code
        {% else %}
            destination_operation_code
        {% endif %}
    {% endset %}

    {% set waste_filter %}
        {% if model_name == "bsdd" %}
            waste_details_code ~* '.*\*$'
            OR waste_details_pop
            or waste_details_is_dangerous  
        {% elif model_name == "bsda" %}
            waste_code ~* '.*\*$'
            OR waste_pop
        {% else %}
            waste_code ~* '.*\*$'
        {% endif %}
    {% endset %}
    
    
    WITH bordereaux AS (
        SELECT
            id,
            {{ processing_operation_column_name }} as "processing_operation_code",
            case
                when {{ quantity_column_name }} > 60 then {{ quantity_column_name }} / 1000
                else {{ quantity_column_name }}
            END as "quantity",
            {{ date_column_name }}
        FROM
            {{ ref(model_name) }}
        WHERE
            {{ date_column_name }} < DATE_TRUNC('week', now())
            {% if not model_name == "bsff_packaging"%}
            AND (
                {{ waste_filter }}
            )
            AND status != 'DRAFT'
            AND is_deleted is false
            {% endif %}
    )
    SELECT
        DATE_TRUNC(
            'week',
            {{ date_column_name }}
        ) AS "week",
        COUNT(id) AS {{ count_name }},
        sum(quantity) as {{ quantity_name }}
        {% if "processed" in count_name  %}
        ,COUNT(id) FILTER (WHERE processing_operation_code in {{ non_final_processing_operation_codes }}) AS {{ count_name }}_non_final_operation,    
        COUNT(id) FILTER (WHERE processing_operation_code not in {{ non_final_processing_operation_codes }}) AS {{ count_name }}_final_operation,
        sum(quantity) FILTER (WHERE processing_operation_code in {{ non_final_processing_operation_codes }}) AS {{ quantity_name }}_non_final_operation,
        sum(quantity) FILTER (WHERE processing_operation_code not in {{ non_final_processing_operation_codes }}) AS {{ quantity_name }}_final_operation 
        {% endif %}
    FROM
        bordereaux
    GROUP BY
        DATE_TRUNC(
            'week',
            {{ date_column_name }}
        )
    ORDER BY
        DATE_TRUNC(
            'week',
            {{ date_column_name }}
        )
{%- endmacro %}
