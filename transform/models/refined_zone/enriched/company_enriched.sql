select
    c.id,
    c.siret,
    c."name",
    c.address,
    c.allowbsdasritakeoverwithoutsignature,
    c.brokerreceiptid,
    c.codenaf,
    c.companytypes,
    c.contact,
    c.contactemail,
    c.contactphone,
    c.createdat,
    c.ecoorganismeagreements,
    c.gerepid,
    c.givenname,
    c.latitude,
    c.longitude,
    c.securitycode,
    c.traderreceiptid,
    c.transporterreceiptid,
    c.updatedat,
    c.vatnumber,
    c.verificationcode,
    c.verificationcomment,
    c.verificationmode,
    c.verificationstatus,
    c.verifiedat,
    c.vhuagrementbroyeurid,
    c.vhuagrementdemolisseurid,
    c.website,
    etabs."codeCommuneEtablissement",
    etabs."codeCommune2Etablissement",
    etabs."etatAdministratifEtablissement",
    comm.dep as code_departement,
    comm.reg as code_region,
    comm.arr as code_arrondissement
from
    raw_zone_trackdechets.company c
left join raw_zone_insee.stock_etablissement etabs
left join raw_zone_insee.commune comm
on
    coalesce(etabs."codeCommuneEtablissement", etabs."codeCommune2Etablissement")= comm.com
on
    c.siret = etabs.siret
