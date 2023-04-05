{% macro create_bordereaux_counts(
        model_name,
        date_column_name,
        count_name
    ) -%}

    {% set non_final_processing_operation_codes %}
    (
        'D 9',
        'D 13',
        'D 14',
        'D 15',
        'R 12',
        'R 13'
    )
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
            regexp_replace({{ processing_operation_column_name }},'([RD])([0-9]{1,2})', '\1 \2') as "processing_operation_code",
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
            COUNT(id) AS {{ count_name }}
            {% if "processed" in count_name  %}
            ,COUNT(id) FILTER (WHERE processing_operation_code in {{ non_final_processing_operation_codes }}) AS {{ count_name }}_final_operation     
            ,COUNT(id) FILTER (WHERE processing_operation_code not in {{ non_final_processing_operation_codes }}) AS {{ count_name }}_non_final_operation    
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
