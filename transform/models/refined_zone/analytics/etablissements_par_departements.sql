select
    commu."dep",
    count(*) as "num_etabs"
from
    "raw_zone_insee"."stock_etablissement" s
left join "raw_zone_insee"."commune" commu on
    coalesce (s."codeCommuneEtablissement",s."codeCommune2Etablissement") = commu."com"
group by
    commu."dep"