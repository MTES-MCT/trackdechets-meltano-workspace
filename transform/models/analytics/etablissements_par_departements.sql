select
    commu."dep",
    count(*) as "num_etabs"
from
    "raw_zone"."stock_etablissement_sirene" s
left join "raw_zone"."commune" commu on
    coalesce (s."codeCommuneEtablissement",s."codeCommune2Etablissement") = commu."com"
group by
    commu."dep"