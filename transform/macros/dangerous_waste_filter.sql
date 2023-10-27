{% macro dangerous_waste_filter(
        model_name
    ) -%}

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

{%- endmacro %}