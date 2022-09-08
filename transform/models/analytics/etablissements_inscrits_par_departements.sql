select
    commu."dep",
    count(*) as "num_etabs"
from
    "raw_zone"."company" c
left join "raw_zone"."stock_etablissement_sirene" s on
    c."siret" = s."siret"
left join "raw_zone"."commune" commu on
    coalesce (s."codeCommuneEtablissement",s."codeCommune2Etablissement") = commu."com"
group by
    commu."dep"