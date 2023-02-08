select
    "raw_zone_insee"."commune"."dep",
    count(*) as "num_etabs"
from
    "raw_zone_trackdechets"."company"
left join "raw_zone_insee"."stock_etablissement" on
    "raw_zone_trackdechets"."company"."siret"
    = "raw_zone_insee"."stock_etablissement"."siret"
left join "raw_zone_insee"."commune" on
    coalesce(
        "raw_zone_insee"."stock_etablissement"."codeCommuneEtablissement",
        "raw_zone_insee"."stock_etablissement"."codeCommune2Etablissement"
    )
    = "raw_zone_insee"."commune"."com"
group by
    "raw_zone_insee"."commune"."dep"
