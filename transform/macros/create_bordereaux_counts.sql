{% macro create_bordereaux_counts(
        model_name,
        date_column_name,
        count_name
    ) -%}
    WITH bordereaux AS (
        SELECT
            id,
            {{ date_column_name }}
        FROM
            {{ ref(model_name) }}
        WHERE
            is_deleted = FALSE
            AND status != 'DRAFT'
            AND (
                waste_details_code ~* '.*\*$'
                OR waste_details_pop
            )
            AND {{ date_column_name }} < DATE_TRUNC('week', now()))
        SELECT
            DATE_TRUNC(
                'week',
                {{ date_column_name }}
            ) AS "week",
            COUNT(id) AS {{ count_name }}
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
