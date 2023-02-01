{{ config(
  pre_hook = "{{ create_indexes_for_source(['siret']) }}"
) }}
SELECT
    siren,
    nic,
    siret,
    "statutDiffusionEtablissement" as statut_diffusion_etablissement,
    cast("dateCreationEtablissement" as date) as date_creation_etablissement,
    "trancheEffectifsEtablissement" as tranche_effectifs_etablissement,
    cast("anneeEffectifsEtablissement" as integer) as annee_effectifs_etablissement,
    "activitePrincipaleRegistreMetiersEtablissement" as activite_principale_registre_metiers_etablissement,
    cast("dateDernierTraitementEtablissement" as timestamp) as date_dernier_traitement_etablissement,
    cast("etablissementSiege" as bool) as etablissement_siege,
    cast("nombrePeriodesEtablissement" as int) as nombre_periodes_etablissement,
    "complementAdresseEtablissement" as complement_adresse_etablissement,
    "numeroVoieEtablissement" as numero_voie_etablissement,
    "indiceRepetitionEtablissement" as indice_repetition_etablissement,
    "typeVoieEtablissement" as type_voie_etablissement,
    "libelleVoieEtablissement" as libelle_voie_etablissement,
    "codePostalEtablissement" as code_postal_etablissement,
    "libelleCommuneEtablissement" as libelle_commune_etablissement,
    "libelleCommuneEtrangerEtablissement" as libelle_commune_etranger_etablissement,
    "distributionSpecialeEtablissement" as distribution_speciale_etablissement,
    "codeCommuneEtablissement" as code_commune_etablissement,
    "codeCedexEtablissement" as code_cedex_etablissement,
    "libelleCedexEtablissement" as libelle_cedex_etablissement,
    cast("codePaysEtrangerEtablissement" as integer) as code_pays_etranger_etablissement,
    "libellePaysEtrangerEtablissement" as libelle_pays_etranger_etablissement,
    "complementAdresse2Etablissement" as complement_adresse_2_etablissement,
    "numeroVoie2Etablissement" as numero_voie_2_etablissement,
    "indiceRepetition2Etablissement" as indice_repetition_2_etablissement,
    "typeVoie2Etablissement" as type_voie_2_etablissement,
    "libelleVoie2Etablissement" as libelle_voie_2_etablissement,
    "codePostal2Etablissement" as code_postal_2_etablissement,
    "libelleCommune2Etablissement" as libelle_commune_2_etablissement,
    "libelleCommuneEtranger2Etablissement" as libelle_commune_etranger_2_etablissement,
    "distributionSpeciale2Etablissement" as distribution_speciale_2_etablissement,
    "codeCommune2Etablissement" as code_commune_2_etablissement,
    "codeCedex2Etablissement" as code_cedex_2_etablissement,
    "libelleCedex2Etablissement" as libelle_cedex_2_etablissement,
    cast("codePaysEtranger2Etablissement" as integer) as code_pays_etranger_2_etablissement,
    "libellePaysEtranger2Etablissement" as libelle_pays_etranger_2_etablissement,
    cast("dateDebut" as date) as date_debut,
    "etatAdministratifEtablissement" as etat_administratif_etablissement,
    "enseigne1Etablissement" as enseigne_1_etablissement,
    "enseigne2Etablissement" as enseigne_2_etablissement,
    "enseigne3Etablissement" as enseigne_3_etablissement,
    "denominationUsuelleEtablissement" as denomination_usuelle_etablissement,
    "activitePrincipaleEtablissement" as activite_principale_etablissement,
    "nomenclatureActivitePrincipaleEtablissement" as nomenclature_activite_principale_etablissement,
    "caractereEmployeurEtablissement" as caractere_employeur_etablissement
FROM
    {{ source('raw_zone_insee', 'stock_etablissement') }}