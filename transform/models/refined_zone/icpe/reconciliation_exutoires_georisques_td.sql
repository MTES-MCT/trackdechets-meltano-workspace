with icpe_data as (
    select
        siret_clean as siret,
        array_agg(distinct code_s3ic) as codes_s3ic,
        max(nom_etablissement_icpe) as nom_etablissement,
        array_agg(
            distinct rubrique || coalesce('-' || alinea, '')
        ) as rubriques_autorises
    from
        {{ ref('icpe_siretise') }}
    where
        (
            (rubrique in ('2770', '2790'))
            or (
                rubrique = '2760'
                and alinea = '1'
            )
        )
        and (
            en_vigueur is true
            or en_vigueur is null
        )
        and (etat_administratif_etablissement != 'F')
    group by
        siret
),

bsdd_data as (
    select
        b.recipient_company_siret as siret,
        max(b.recipient_company_name) as company_name,
        array_agg(distinct b.processing_operation_done) as code_operation,
        count(id) as nombre_bordereaux,
        sum(quantity_received) as quantite
    from
        {{ ref('bsdd') }} as b
    where
        b.processing_operation_done not in (
            'D13',
            'D 13',
            'D14',
            'D 14',
            'D15',
            'D 15',
            'R12',
            'R 12',
            'R13',
            'R 13'
        )
    group by
        siret
    order by
        siret
),

bsda_data as (
    select
        bsda.destination_company_siret as siret,
        max(bsda.destination_company_name) as company_name,
        array_agg(distinct bsda.destination_operation_code) as code_operation,
        count(id) as nombre_bordereaux,
        sum(destination_reception_weight) as quantite
    from
        {{ ref('bsda') }}
    where
        bsda.destination_operation_code in ('D5', 'D 5', 'R 5', 'R5')
    group by
        siret
    order by
        siret
)

select
    max(case
        when trusted_zone_trackdechets.company.siret is not null then 1
        else 0
    end)::bool as "inscrit_sur_trackdechets",
    coalesce(
        icpe_data.siret,
        bsdd_data.siret,
        bsda_data.siret,
        trusted_zone_trackdechets.company.siret
    ) as siret,
    max(
        coalesce(
            icpe_data.nom_etablissement,
            trusted_zone_trackdechets.company.name,
            bsdd_data.company_name,
            bsda_data.company_name
        )
    ) as nom_etablissement,
    max(icpe_data.codes_s3ic) as codes_s3ic,
    max(icpe_data.rubriques_autorises) as rubriques_autorises,
    max(bsdd_data.nombre_bordereaux) as "num_bsdd",
    max(bsdd_data.quantite) as "tonnage_bsdd",
    max(bsdd_data.code_operation) as "operations_effectues_bsdd",
    max(bsda_data.nombre_bordereaux) as "num_bsda",
    max(bsda_data.quantite) as "tonnage_bsda",
    max(bsda_data.code_operation) as "operations_effectues_bsda"
from
    icpe_data
full outer join bsdd_data on
    icpe_data.siret = bsdd_data.siret
full outer join bsda_data on
    bsdd_data.siret = bsda_data.siret
left join trusted_zone_trackdechets.company
    on
        coalesce(icpe_data.siret, bsdd_data.siret, bsda_data.siret)
        = trusted_zone_trackdechets.company.siret
group by
    coalesce(
        icpe_data.siret,
        bsdd_data.siret,
        bsda_data.siret,
        trusted_zone_trackdechets.company.siret
    )
