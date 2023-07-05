{{
  config(
    materialized = 'table',
    indexes = [  {'columns': ['siret'] }]
    )
}}

with gerep_traiteurs as (
    select
        *,
        row_number() over (
            partition by code_etablissement
            order by
                annee desc
        )
    from
        {{ ref('gerep_traiteurs_2021') }}
)

select
    i.code_aiot,
    i.raison_sociale,
    i.num_siret                     as siret_icpe,
    gt.siret                        as siret_gerep,
    i.etat_activite,
    i.id_lex_aiot_regime,
    i.longitude,
    i.latitude,
    i.code_postal,
    i.code_insee,
    coalesce(i.num_siret, gt.siret) as siret
from
    {{ ref('installations') }} as i
left join gerep_traiteurs as gt on
    i.code_aiot = gt.code_etablissement and gt.row_number = 1
