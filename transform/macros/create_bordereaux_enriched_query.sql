{% macro create_bordereaux_enriched_query(
        model_name,
        use_recipient
    ) -%}

{% if use_recipient %}
    {% set destination_prefix = 'recipient' %}
{% else %}
    {% set destination_prefix = 'destination' %}
{% endif %}



WITH etabs AS (
    {{ create_sirene_etabs_enriched_statement() }}
),
coords as (
    {{ create_laposte_enriched_statement() }}
),
filtered_form as (
    select * from {{ ref(model_name) }} b
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where b.updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),
firts_join as (
    SELECT
    b.*,
    etabs.code_commune                      AS "emitter_commune",
    etabs.code_departement                  AS "emitter_departement",
    etabs.code_region                       AS "emitter_region",
    coords.latitude                       AS "emitter_latitude",
    coords.longitude                       AS "emitter_longitude",
    etabs.activite_principale_etablissement AS "emitter_naf"
    FROM
        filtered_form
        AS b
    LEFT JOIN etabs
        ON b.emitter_company_siret = etabs.siret
    LEFT JOIN coords
        ON coords.code_commune_insee = etabs.code_commune
)
SELECT
    b.*,
    etabs.code_commune                      AS "{{destination_prefix}}_commune",
    etabs.code_departement                  AS "{{destination_prefix}}_departement",
    etabs.code_region                       AS "{{destination_prefix}}_region",
    coords.latitude                       AS "{{destination_prefix}}_latitude",
    coords.longitude                       AS "{{destination_prefix}}_longitude",
    etabs.activite_principale_etablissement AS "{{destination_prefix}}_naf"
FROM
    firts_join
    AS b
LEFT JOIN etabs
    ON b.{{destination_prefix}}_company_siret = etabs.siret
LEFT JOIN coords
    ON coords.code_commune_insee = etabs.code_commune
{%- endmacro %}
