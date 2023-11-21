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
        max(ir.code_insee)            as code_commune,
        max(ir."adresse1")            as "adresse1",
        max(ir."adresse2")            as "adresse2",
        max(ir."code_postal")         as "code_postal",
        max(ir."commune")             as "commune"
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
),

stats_rubriques as (
    select
        siret,
        jsonb_agg(
            json_build_object(
                'quantite_traitee',
                idpw.quantite_traitee,
                'day_of_processing',
                idpw.day_of_processing
            )
            order by
                idpw.day_of_processing asc
        ) filter (
            where
            idpw.rubrique = '2760-1'
        ) as stats_2760,
        max(quantite_autorisee) filter (
            where
            idpw.rubrique = '2760-1'
        ) as quantite_autorisee_2760,
        jsonb_agg(
            json_build_object(
                'quantite_traitee',
                idpw.quantite_traitee,
                'day_of_processing',
                idpw.day_of_processing
            )
            order by
                idpw.day_of_processing asc
        ) filter (
            where
            idpw.rubrique = '2770'
        ) as stats_2770,
        max(quantite_autorisee) filter (
            where
            idpw.rubrique = '2770'
        ) as quantite_autorisee_2770
    from
        {{ ref('installations_daily_processed_wastes') }} as idpw
    group by
        idpw.siret
)

select
    coords.*,
    i.siret,
    i.codes_aiot,
    i.rubriques_json,
    i.raison_sociale,
    i."adresse1",
    i."adresse2",
    i."code_postal",
    i."commune",
    sr.stats_2760,
    sr.quantite_autorisee_2760,
    sr.stats_2770,
    sr.quantite_autorisee_2770
from
    installations_data_filtered as i
left join stats_rubriques as sr on i.siret = sr.siret
left join {{ ref('stock_etablissement') }} as se
    on
        i.siret = se.siret
left join coords on
    coalesce(
        se.code_commune_etablissement,
        i.code_commune
    ) = coords.code_commune_insee
