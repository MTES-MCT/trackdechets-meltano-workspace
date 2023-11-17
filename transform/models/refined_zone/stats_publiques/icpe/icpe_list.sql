{{
  config(
    materialized = 'table',
    )
}}

with installations_data as (
    select
        ir.siret,
        array_agg(distinct code_aiot) as codes_aiot,
        jsonb_agg(
            json_build_object(
                'rubrique',
                rubrique || coalesce(
                    '-' || alinea,
                    ''
                ),
                'quantite_autorisee',
                ir.quantite_totale,
                'unite',
                ir.unite,
                'nature',
                ir.nature
            )
            order by
                rubrique,
                alinea
        )                             as rubriques_json,
        max(ir.raison_sociale)        as raison_sociale,
        max(ir.code_insee)            as code_commune
    from
        {{ ref('installations_rubriques') }} as ir
    where
        ir.siret is not null
        and ir.etat_activite in (
            'En exploitation avec titre', 'En fin d’exploitation'
        )
        and ir.regime in ('Autorisation', 'Enregistrement')
    group by
        ir.siret
),

installations_data_filtered as (
    select *
    from
        installations_data as i
    where
        i.rubriques_json @> '[{"rubrique":"2760-1"}]'
        or i.rubriques_json @> '[{"rubrique":"2770"}]'
        or i.rubriques_json @> '[{"rubrique":"2790"}]'
),

coords as (
    select
        bcp.code_commune_insee,
        avg(bcp.latitude)  as latitude,
        avg(bcp.longitude) as longitude
    from
        {{ ref('base_codes_postaux') }} as bcp
    group by
        bcp.code_commune_insee
)

select
    coords.*,
    i.siret,
    i.codes_aiot,
    i.rubriques_json,
    i.raison_sociale
from
    installations_data_filtered as i
left join {{ ref('stock_etablissement') }} as se
    on
        i.siret = se.siret
left join coords on
    coalesce(
        se.code_commune_etablissement,
        i.code_commune
    ) = coords.code_commune_insee
