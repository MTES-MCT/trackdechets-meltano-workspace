{{
  config(
    materialized = 'table',
    )
}}


with emetteurs as (
    select
        "_bs_type",
        emitter_company_siret as siret,
        count(*)              as num_emetteur
    from {{ ref('bordereaux_enriched') }} as be
    where be.received_at >= now() - interval '1 year'
    group by 1, 2
    order by 3 desc
),

transporteurs as (
-- Gestion du multimodal
    select
        "_bs_type",
        coalesce(
            be.transporter_company_siret,
            bsddt.transporter_company_siret,
            bsdat.transporter_company_siret,
            bsfft.transporter_company_siret
        ) as siret,
        count(
            distinct be.id
        ) as num_transporter
    from {{ ref('bordereaux_enriched') }} as be
    left join
        {{ ref('bsdd_transporter') }} as bsddt
        on be.id = bsddt.form_id and be."_bs_type" = 'BSDD'
    left join
        {{ ref('bsda_transporter') }} as bsdat
        on be.id = bsdat.bsda_id and be."_bs_type" = 'BSDA'
    left join
        {{ ref('bsff_transporter') }} as bsfft
        on be.id = bsfft.bsff_id and be."_bs_type" = 'BSFF'
    where be.received_at >= now() - interval '1 year'
    group by 1, 2
    order by 3 desc
),

destinataires as (
    select
        "_bs_type",
        destination_company_siret as siret,
        count(*)                  as num_destinataire
    from {{ ref('bordereaux_enriched') }} as be
    where be.received_at >= now() - interval '1 year'
    group by 1, 2
    order by 3 desc
),

all_stats as (
    select
        coalesce(
            e."_bs_type", t."_bs_type", d."_bs_type"
        )                               as "_bs_type",
        coalesce(
            e.siret, t.siret, d.siret
        )                               as siret,
        num_emetteur,
        num_transporter,
        num_destinataire,
        coalesce(num_emetteur, 0)
        + coalesce(num_transporter, 0)
        + coalesce(num_destinataire, 0) as total
    from emetteurs as e
    full outer join
        transporteurs as t
        on e.siret = t.siret and e."_bs_type" = t."_bs_type"
    full outer join
        destinataires as d
        on
            coalesce(e.siret, t.siret) = d.siret
            and coalesce(e."_bs_type", t."_bs_type") = d."_bs_type"
    where
        coalesce(e.siret, t.siret, d.siret) is not null
        and coalesce(e.siret, t.siret, d.siret) != ''
    order by 1, 6 desc
),
--- Sélection de cohortes indépendantes pour chaque type de bordereaux (TOP 100 SIRETs en nombre de mentions)
top_bsdd as (
    select *
    from all_stats
    where "_bs_type" = 'BSDD'
    order by total desc
    limit 100
),

top_bsda as (
    (
        select *
        from all_stats where
            "_bs_type" = 'BSDA'
            and siret not in (select siret from top_bsdd)
        order by total desc
        limit 100
    )
    union all
    select *
    from top_bsdd
),

top_bsdasri as (
    (
        select *
        from all_stats where
            "_bs_type" = 'BSDASRI'
            and siret not in (select siret from top_bsda)
        order by total desc
        limit 100
    )
    union all
    select *
    from top_bsda
),

top_bsff as (
    (
        select *
        from all_stats where
            "_bs_type" = 'BSFF'
            and siret not in (select siret from top_bsdasri)
        order by total desc
        limit 100
    )
    union all
    select *
    from top_bsdasri
),

top_bsvhu as (
    (
        select *
        from all_stats where
            "_bs_type" = 'BSVHU'
            and siret not in (select siret from top_bsff)
        order by total desc
        limit 100
    )
    union all
    select *
    from top_bsff
)

select * from top_bsvhu
