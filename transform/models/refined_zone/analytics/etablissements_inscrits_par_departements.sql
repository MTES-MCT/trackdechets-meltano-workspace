select
    commu."dep",
    count(*) as "num_etabs"
from
    "raw_zone_trackdechets"."company" c
left join "raw_zone_insee"."stock_etablissement" s on
    c."siret" = s."siret"
left join "raw_zone_insee"."commune" commu on
    coalesce (s."codeCommuneEtablissement",s."codeCommune2Etablissement") = commu."com"
group by
    commu."dep"