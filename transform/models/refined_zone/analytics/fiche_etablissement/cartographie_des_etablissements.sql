{{
  config(
    materialized = 'table',
    indexes = [ 
        {
            'columns': ['siret'],
            'unique': True 
        },
        {
            'columns': ['coords'],
            'type': 'GIST' 
        },
    ],
    tags = ['fiche_etablissement']
    )
}}

with stats as (
    select
        sbs.siret,
        coalesce(
            sbs.processing_operations_as_destination_bsdd, array[]::text[]
        ) as processing_operations_bsdd,
        coalesce(
            sbs.processing_operations_as_destination_bsdnd, array[]::text[]
        ) as processing_operations_bsdnd,
        coalesce(
            sbs.processing_operations_as_destination_bsda, array[]::text[]
        ) as processing_operations_bsda,
        coalesce(
            sbs.processing_operations_as_destination_bsff, array[]::text[]
        ) as processing_operations_bsff,
        coalesce(
            sbs.processing_operations_as_destination_bsdasri, array[]::text[]
        ) as processing_operations_bsdasri,
        coalesce(
            sbs.processing_operations_as_destination_bsvhu, array[]::text[]
        ) as processing_operations_bsvhu,
        coalesce(
            sbs.dnd_processing_operations_as_destination, array[]::text[]
        ) as processing_operation_dnd,
        coalesce(
            sbs.texs_processing_operations_as_destination, array[]::text[]
        ) as processing_operation_texs,
        coalesce(
            sbs.waste_codes_as_destination, array[]::text[]
        ) as waste_codes_bordereaux,
        coalesce(
            sbs.dnd_waste_codes_as_destination, array[]::text[]
        ) as waste_codes_dnd_statements,
        coalesce(
            sbs.texs_waste_codes_as_destination, array[]::text[]
        ) as waste_codes_texs_statements,
        coalesce(
            sbs.waste_codes_as_destination
            || sbs.dnd_waste_codes_as_destination
            || sbs.texs_waste_codes_as_destination,
            array[]::text[]
        ) as waste_codes_processed,
        coalesce(
            sbs.num_bsdd_as_emitter > 0, false
        ) as bsdd_emitter,
        coalesce(
            sbs.num_bsdd_as_transporter > 0, false
        ) as bsdd_transporter,
        coalesce(
            sbs.num_bsdd_as_destination > 0, false
        ) as bsdd_destination,
        coalesce(
            sbs.num_bsdd_as_emitter > 0
            or sbs.num_bsdd_as_transporter > 0
            or sbs.num_bsdd_as_destination > 0, false
        ) as bsdd,
        coalesce(
            sbs.num_bsdnd_as_emitter > 0, false
        ) as bsdnd_emitter,
        coalesce(
            sbs.num_bsdnd_as_transporter > 0, false
        ) as bsdnd_transporter,
        coalesce(
            sbs.num_bsdnd_as_destination > 0, false
        ) as bsdnd_destination,
        coalesce(
            sbs.num_bsdnd_as_emitter > 0
            or sbs.num_bsdnd_as_transporter > 0
            or sbs.num_bsdnd_as_destination > 0, false
        ) as bsdnd,
        coalesce(
            sbs.num_bsda_as_emitter > 0, false
        ) as bsda_emitter,
        coalesce(
            sbs.num_bsda_as_transporter > 0, false
        ) as bsda_transporter,
        coalesce(
            sbs.num_bsda_as_destination > 0, false
        ) as bsda_destination,
        coalesce(
            sbs.num_bsda_as_emitter > 0
            or sbs.num_bsda_as_transporter > 0
            or sbs.num_bsda_as_destination > 0, false
        ) as bsda,
        coalesce(
            sbs.num_bsff_as_emitter > 0, false
        ) as bsff_emitter,
        coalesce(
            sbs.num_bsff_as_transporter > 0, false
        ) as bsff_transporter,
        coalesce(
            sbs.num_bsff_as_destination > 0, false
        ) as bsff_destination,
        coalesce(
            sbs.num_bsff_as_emitter > 0
            or sbs.num_bsff_as_transporter > 0
            or sbs.num_bsff_as_destination > 0, false
        ) as bsff,
        coalesce(
            sbs.num_bsdasri_as_emitter > 0, false
        ) as bsdasri_emitter,
        coalesce(
            sbs.num_bsdasri_as_transporter > 0, false
        ) as bsdasri_transporter,
        coalesce(
            sbs.num_bsdasri_as_destination > 0, false
        ) as bsdasri_destination,
        coalesce(
            sbs.num_bsdasri_as_emitter > 0
            or sbs.num_bsdasri_as_transporter > 0
            or sbs.num_bsdasri_as_destination > 0, false
        ) as bsdasri,
        coalesce(
            sbs.num_bsvhu_as_emitter > 0, false
        ) as bsvhu_emitter,
        coalesce(
            sbs.num_bsvhu_as_transporter > 0, false
        ) as bsvhu_transporter,
        coalesce(
            sbs.num_bsvhu_as_destination > 0, false
        ) as bsvhu_destination,
        coalesce(
            sbs.num_bsvhu_as_emitter > 0
            or sbs.num_bsvhu_as_transporter > 0
            or sbs.num_bsvhu_as_destination > 0, false
        ) as bsvhu,
        coalesce(
            sbs.num_dnd_statements_as_emitter > 0, false
        ) as dnd_emitter,
        coalesce(
            sbs.num_texs_statements_as_destination > 0, false
        ) as dnd_destination,
        coalesce(
            sbs.num_dnd_statements_as_destination > 0
            or sbs.num_dnd_statements_as_emitter > 0, false
        ) as dnd,
        coalesce(
            sbs.num_texs_statements_as_destination > 0, false
        ) as texs_destination,
        coalesce(
            sbs.num_texs_statements_as_emitter > 0, false
        ) as texs_emitter,
        coalesce(
            sbs.num_texs_statements_as_destination > 0
            or sbs.num_texs_statements_as_emitter > 0, false
        ) as texs,
        coalesce(
            sbs.num_ssd_statements_as_emitter > 0, false
        ) as ssd,
        coalesce(
            sbs.num_pnttd_statements_as_destination > 0, false
        ) as pnttd
    from {{ ref('statistics_by_siret') }} as sbs
    where char_length(sbs.siret) = 14
),

joined as (
    select
        s.*,
        se.code_commune_etablissement                  as code_commune_insee,
        cgc.code_departement                           as code_departement_insee,
        cgc.code_region                                as code_region_insee,
        c.address                                      as adresse_td,
        c.latitude                                     as latitude_td,
        c.longitude                                    as longitude_td,
        cban.latitude                                  as latitude_ban,
        cban.longitude                                 as longitude_ban,
        coalesce(c.company_types, array[]::text[])    as profils,
        coalesce(
            c.collector_types, array[]::text[]
        )                                              as profils_collecteur,
        coalesce(
            c.waste_processor_types, array[]::text[]
        )                                              as profils_installation,
        coalesce(
            c.waste_vehicles_types, array[]::text[]
        )                                              as profils_installation_traitement_vhu,
        coalesce(et.num_texs_dd_as_emitter > 0, false) as texs_dd_emitter,
        coalesce(
            et.num_texs_dd_as_transporter > 0, false
        )                                              as texs_dd_transporter,
        coalesce(
            et.num_texs_dd_as_destination > 0, false
        )                                              as texs_dd_destination,
        coalesce(
            et.num_texs_dd_as_emitter > 0
            or et.num_texs_dd_as_transporter > 0
            or et.num_texs_dd_as_destination > 0, false
        )                                              as texs_dd,
        st_setsrid(
            st_point(
                coalesce(c.longitude, cban.longitude),
                coalesce(c.latitude, cban.latitude)
            ),
            4326
        )                                              as coords,
        nullif(
            coalesce(se.complement_adresse_etablissement || ' ', '')
            || coalesce(se.numero_voie_etablissement || ' ', '')
            || coalesce(se.indice_repetition_etablissement || ' ', '')
            || coalesce(se.type_voie_etablissement || ' ', '')
            || coalesce(se.libelle_voie_etablissement || ' ', '')
            || coalesce(se.code_postal_etablissement || ' ', '')
            || coalesce(se.libelle_commune_etablissement || ' ', '')
            || coalesce(se.libelle_commune_etranger_etablissement || ' ', '')
            || coalesce(se.distribution_speciale_etablissement, ''), ''
        )                                              as adresse_insee,
        coalesce(
            c.name, se.enseigne_1_etablissement,
            se.enseigne_2_etablissement,
            se.enseigne_3_etablissement,
            se.denomination_usuelle_etablissement
        )                                              as nom_etablissement
    from stats as s
    left join {{ ref('etablissements_texs_dd') }} as et on s.siret = et.siret
    left join {{ ref("stock_etablissement") }} as se on s.siret = se.siret
    left join {{ ref("company") }} as c on s.siret = c.siret
    left join
        {{ ref("code_geo_communes") }} as cgc
        on
            se.code_commune_etablissement = cgc.code_commune
            and cgc.type_commune != 'COMD'
    left join
        {{ ref("companies_geocoded_by_ban") }} as cban
        on s.siret = cban.siret and cban.result_status = 'ok'
)

select
    siret,
    nom_etablissement,
    profils,
    profils_collecteur,
    profils_installation,
    bsdd,
    bsdd_emitter,
    bsdd_transporter,
    bsdd_destination,
    bsdnd,
    bsdnd_emitter,
    bsdnd_transporter,
    bsdnd_destination,
    bsda,
    bsda_emitter,
    bsda_transporter,
    bsda_destination,
    bsff,
    bsff_emitter,
    bsff_transporter,
    bsff_destination,
    bsdasri,
    bsdasri_emitter,
    bsdasri_transporter,
    bsdasri_destination,
    bsvhu,
    bsvhu_emitter,
    bsvhu_transporter,
    bsvhu_destination,
    texs_dd,
    texs_dd_emitter,
    texs_dd_transporter,
    texs_dd_destination,
    dnd,
    dnd_emitter,
    dnd_destination,
    texs,
    texs_destination,
    texs_emitter,
    ssd,
    pnttd,
    processing_operations_bsdd,
    processing_operations_bsdnd,
    processing_operations_bsda,
    processing_operations_bsff,
    processing_operations_bsdasri,
    processing_operations_bsvhu,
    processing_operation_dnd,
    processing_operation_texs,
    waste_codes_bordereaux,
    waste_codes_dnd_statements,
    waste_codes_texs_statements,
    waste_codes_processed,
    code_commune_insee,
    code_departement_insee,
    code_region_insee,
    adresse_td,
    adresse_insee,
    latitude_td,
    longitude_td,
    latitude_ban,
    longitude_ban,
    coords
from joined
