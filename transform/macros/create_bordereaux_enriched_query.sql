{% macro create_bordereaux_enriched_query(
        model_name,
        is_temp,
        use_recipient
    ) -%}


{% if is_temp %}
    {% set prefix = 'emitter' %}
{% elif use_recipient %}
    {% set prefix = 'recipient' %}
{% else %}
    {% set prefix = 'destination' %}
{% endif %}

WITH etabs AS (
    {{ create_sirene_etabs_enriched_statement() }}
),
coords as (
    {{ create_laposte_enriched_statement() }}
)
SELECT
    b.*,
    etabs.code_commune                      AS "{{prefix}}_commune",
    etabs.code_departement                  AS "{{prefix}}_departement",
    etabs.code_region                       AS "{{prefix}}_region",
    coords.latitude                       AS "{{prefix}}_latitude",
    coords.longitude                       AS "{{prefix}}_longitude",
    etabs.activite_principale_etablissement AS "{{prefix}}_naf"
FROM
    {{ ref(model_name) }}
    AS b
LEFT JOIN etabs
    ON b.{{prefix}}_company_siret = etabs.siret
LEFT JOIN coords
    ON coords.code_commune_insee = etabs.code_commune
    
{%- endmacro %}
