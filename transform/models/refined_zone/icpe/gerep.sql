with gerep_producteurs as (
    select
        "annee",
        "code_etablissement",
        "numero_siret",
        row_number() over (
            partition by
                "code_etablissement", "numero_siret"
            order by annee desc
        )
    from
        raw_zone_gsheet.gerep_2016_2017_producteurs
),

gerep_traiteurs as (
    select
        "annee",
        "code_etablissement",
        "numero_siret",
        row_number() over (
            partition by
                "code_etablissement", "numero_siret"
            order by annee desc
        )
    from
        raw_zone_gsheet.gerep_2016_2017_traiteurs
),

gerep_producteurs_dedup as (
    select *
    from
        gerep_producteurs
    where "row_number" = 1
),

gerep_traiteurs_dedup as (
    select *
    from
        gerep_traiteurs
    where "row_number" = 1
),

all_data as (
    select * from gerep_producteurs_dedup
    union
    select * from gerep_traiteurs_dedup
)

select
    "code_etablissement" as "codeS3ic",
    "numero_siret",
    case
        when
            length("code_etablissement") = 10 then "code_etablissement"
        when
            position('.' in "code_etablissement") = 0
            then lpad(
                substring(
                    "code_etablissement", -5, length("code_etablissement") + 1
                )
                || '.'
                || substring(
                    "code_etablissement", 5, length("code_etablissement") + 1
                ),
                10,
                '0'
            )
        else
            lpad(code_etablissement, 10, '0')
    end                  as "code_etablissement"
from
    all_data
where length("numero_siret") = 14
