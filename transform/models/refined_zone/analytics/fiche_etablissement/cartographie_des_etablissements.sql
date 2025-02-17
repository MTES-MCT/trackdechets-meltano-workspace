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
        sbs.processing_operations_as_destination_bsdd    as processing_operations_bsdd,
        sbs.processing_operations_as_destination_bsdnd   as processing_operations_bsdnd,
        sbs.processing_operations_as_destination_bsda    as processing_operations_bsda,
        sbs.processing_operations_as_destination_bsff    as processing_operations_bsff,
        sbs.processing_operations_as_destination_bsdasri as processing_operations_bsdasri,
        sbs.processing_operations_as_destination_bsvhu   as processing_operations_bsvhu,
        sbs.dnd_processing_operations_as_destination     as processing_operation_dnd,
        sbs.texs_processing_operations_as_destination    as processing_operation_texs,
        sbs.waste_codes_as_destination as waste_codes_bordereaux,
        sbs.dnd_waste_codes_as_destination as waste_codes_dnd_statements,
        sbs.texs_waste_codes_as_destination as waste_codes_texs_statements,
        sbs.waste_codes_as_destination || sbs.dnd_waste_codes_as_destination || sbs.texs_waste_codes_as_destination as waste_codes_processed,
        sbs.num_bsdd_as_emitter > 0                      as bsdd_emitter,
        sbs.num_bsdd_as_transporter > 0                  as bsdd_transporter,
        sbs.num_bsdd_as_destination > 0                  as bsdd_destination,
        sbs.num_bsdd_as_emitter > 0
        or sbs.num_bsdd_as_transporter > 0
        or sbs.num_bsdd_as_destination > 0               as bsdd,
        sbs.num_bsdnd_as_emitter > 0                     as bsdnd_emitter,
        sbs.num_bsdnd_as_transporter > 0                 as bsdnd_transporter,
        sbs.num_bsdnd_as_destination > 0                 as bsdnd_destination,
        sbs.num_bsdnd_as_emitter > 0
        or sbs.num_bsdnd_as_transporter > 0
        or sbs.num_bsdnd_as_destination > 0              as bsdnd,
        sbs.num_bsda_as_emitter > 0                      as bsda_emitter,
        sbs.num_bsda_as_transporter > 0                  as bsda_transporter,
        sbs.num_bsda_as_destination > 0                  as bsda_destination,
        sbs.num_bsda_as_emitter > 0
        or sbs.num_bsda_as_transporter > 0
        or sbs.num_bsda_as_destination > 0               as bsda,
        sbs.num_bsff_as_emitter > 0                      as bsff_emitter,
        sbs.num_bsff_as_transporter > 0                  as bsff_transporter,
        sbs.num_bsff_as_destination > 0                  as bsff_destination,
        sbs.num_bsff_as_emitter > 0
        or sbs.num_bsff_as_transporter > 0
        or sbs.num_bsff_as_destination > 0               as bsff,
        sbs.num_bsdasri_as_emitter > 0                   as bsdasri_emitter,
        sbs.num_bsdasri_as_transporter > 0               as bsdasri_transporter,
        sbs.num_bsdasri_as_destination > 0               as bsdasri_destination,
        sbs.num_bsdasri_as_emitter > 0
        or sbs.num_bsdasri_as_transporter > 0
        or sbs.num_bsdasri_as_destination > 0            as bsdasri,
        sbs.num_bsvhu_as_emitter > 0                     as bsvhu_emitter,
        sbs.num_bsvhu_as_transporter > 0                 as bsvhu_transporter,
        sbs.num_bsvhu_as_destination > 0                 as bsvhu_destination,
        sbs.num_bsvhu_as_emitter > 0
        or sbs.num_bsvhu_as_transporter > 0
        or sbs.num_bsvhu_as_destination > 0              as bsvhu,
        sbs.num_dnd_statements_as_emitter > 0            as dnd_emitter,
        sbs.num_texs_statements_as_destination > 0       as dnd_destination,
        sbs.num_dnd_statements_as_destination > 0
        or sbs.num_dnd_statements_as_emitter > 0         as dnd,
        sbs.num_texs_statements_as_destination > 0       as texs_destination,
        sbs.num_texs_statements_as_emitter > 0           as texs_emitter,
        sbs.num_texs_statements_as_destination > 0
        or sbs.num_texs_statements_as_emitter > 0        as texs,
        sbs.num_ssd_statements_as_emitter > 0            as ssd,
        sbs.num_pnttd_statements_as_destination > 0      as pnttd
    from {{ ref('statistics_by_siret') }} as sbs
    where char_length(sbs.siret) = 14
),

joined as (
    select
        s.*,
        c.company_types                      as profils,
        c.collector_types                    as profils_collecteur,
        c.waste_processor_types              as profils_installation,
        c.waste_vehicles_types               as profils_installation_traitement_vhu,
        se.code_commune_etablissement        as code_commune_insee,
        cgc.code_departement                 as code_departement_insee,
        cgc.code_region                      as code_region_insee,
        c.address                            as adresse_td,
        c.latitude                           as latitude_td,
        c.longitude                          as longitude_td,
        cban.latitude                        as latitude_ban,
        cban.longitude                       as longitude_ban,
        et.num_texs_dd_as_emitter > 0        as texs_dd_emitter,
        et.num_texs_dd_as_transporter > 0    as texs_dd_transporter,
        et.num_texs_dd_as_destination > 0    as texs_dd_destination,
        et.num_texs_dd_as_emitter > 0
        or et.num_texs_dd_as_transporter > 0
        or et.num_texs_dd_as_destination > 0 as texs_dd,
        st_setsrid(
            st_point(
                coalesce(c.longitude, cban.longitude),
                coalesce(c.latitude, cban.latitude)
            ),
            4326
        )                                    as coords,
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
        )                                    as adresse_insee,
        coalesce(
            c.name, se.enseigne_1_etablissement,
            se.enseigne_2_etablissement,
            se.enseigne_3_etablissement,
            se.denomination_usuelle_etablissement
        )                                    as nom_etablissement
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
