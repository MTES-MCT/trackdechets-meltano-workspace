{{ config(
    indexes = [ {'columns': ['siret'] }]
) }}

WITH decheteries AS (

    SELECT
        C.siret,
        MAX(
            C.name
        ) AS company_name,
        MAX(
            C.contact_email
        ) AS "contact_email",
        MAX(
            C.company_types :: text
        ) AS "company_types",
        MAX(
            u.email
        ) AS "email_admin"
    FROM
        {{ ref('company') }} C
        LEFT JOIN {{ ref('company_association') }}
        ca
        ON ca.company_id = C.id
        LEFT JOIN {{ ref('user') }}
        u
        ON ca.user_id = u.id
    WHERE
        C.company_types ? 'WASTE_CENTER'
        AND ca."role" = 'ADMIN'
    GROUP BY
        siret
),
joined AS (
    SELECT
        d.siret,
        COUNT(*) filter (
            WHERE
                be."_bs_type" = 'BSDD'
        ) AS "num_bsdd_destinataire",
        COUNT(*) filter (
            WHERE
                be."_bs_type" = 'BSDA'
        ) AS "num_bsda_destinataire",
        COUNT(*) filter (
            WHERE
                be."_bs_type" = 'BSFF'
        ) AS "num_bsff_destinataire",
        COUNT(*) filter (
            WHERE
                be."_bs_type" = 'BSDASRI'
        ) AS "num_bsdasri_destinataire",
        COUNT(*) filter (
            WHERE
                be."_bs_type" = 'BSVHU'
        ) AS "num_bsvhu_destinataire"
    FROM
        decheteries d
        LEFT JOIN {{ ref('bordereaux_enriched') }}
        be
        ON d.siret = be.destination_company_siret
    WHERE
        be.processed_at IS NOT NULL
    GROUP BY
        d.siret
),
joined_2 AS (
    SELECT
        d.siret,
        COUNT(*) filter (
            WHERE
                be."_bs_type" = 'BSDD'
        ) AS "num_bsdd_emetteur",
        COUNT(*) filter (
            WHERE
                be."_bs_type" = 'BSDA'
        ) AS "num_bsda_emetteur",
        COUNT(*) filter (
            WHERE
                be."_bs_type" = 'BSFF'
        ) AS "num_bsff_emetteur",
        COUNT(*) filter (
            WHERE
                be."_bs_type" = 'BSDASRI'
        ) AS "num_bsdasri_emetteur",
        COUNT(*) filter (
            WHERE
                be."_bs_type" = 'BSVHU'
        ) AS "num_bsvhu_emetteur"
    FROM
        decheteries d
        LEFT JOIN {{ ref('bordereaux_enriched') }}
        be
        ON d.siret = be.emitter_company_siret
    WHERE
        be.processed_at IS NOT NULL
    GROUP BY
        d.siret
),
final_ AS (
    SELECT
        coalesce(joined.siret,joined_2.siret) as siret,
        coalesce(joined.num_bsdd_destinataire,0) as num_bsdd_destinataire,
        coalesce(joined.num_bsda_destinataire,0) as num_bsda_destinataire,
        coalesce(joined.num_bsff_destinataire,0) as num_bsff_destinataire,
        coalesce(joined.num_bsdasri_destinataire,0) as num_bsdasri_destinataire,
        coalesce(joined.num_bsvhu_destinataire,0) as num_bsvhu_destinataire,
        coalesce(joined_2.num_bsdd_emetteur,0) as num_bsdd_emetteur,
        coalesce(joined_2.num_bsda_emetteur,0) as num_bsda_emetteur,
        coalesce(joined_2.num_bsff_emetteur,0) as num_bsff_emetteur,
        coalesce(joined_2.num_bsdasri_emetteur,0) as num_bsdasri_emetteur,
        coalesce(joined_2.num_bsvhu_emetteur,0) as num_bsvhu_emetteur,
        COALESCE(
            num_bsdd_destinataire,
            0
        ) + COALESCE(
            num_bsdd_emetteur,
            0
        ) AS "num_bsdd",
        COALESCE(
            num_bsda_destinataire,
            0
        ) + COALESCE(
            num_bsda_emetteur,
            0
        ) AS "num_bsda",
        COALESCE(
            num_bsff_destinataire,
            0
        ) + COALESCE(
            num_bsff_emetteur,
            0
        ) AS "num_bsff",
        COALESCE(
            num_bsdasri_destinataire,
            0
        ) + COALESCE(
            num_bsdasri_emetteur,
            0
        ) AS "num_bsdasri",
        COALESCE(
            num_bsvhu_destinataire,
            0
        ) + COALESCE(
            num_bsvhu_emetteur,
            0
        ) AS "num_bsvhu"
    FROM
        joined
        full OUTER JOIN joined_2 USING (siret)
)
SELECT
    d.siret,
    d.company_name AS "Nom de l'établissement",
    d.company_types AS "Profils de l'établissement",
    d.contact_email AS "E-mail de contact de l'établissement",
    d."email_admin" AS "E-mail de l'admin de l'établissement",
    final_.num_bsdd_destinataire,
    final_.num_bsda_destinataire,
    final_.num_bsff_destinataire,
    final_.num_bsdasri_destinataire,
    final_.num_bsvhu_destinataire,
    final_.num_bsdd_emetteur,
    final_.num_bsda_emetteur,
    final_.num_bsff_emetteur,
    final_.num_bsdasri_emetteur,
    final_.num_bsvhu_emetteur,
    num_bsdd + num_bsda + num_bsff + num_bsdasri + num_bsvhu AS "total_bordereaux"
FROM
    decheteries d
    left join final_ using (siret)
