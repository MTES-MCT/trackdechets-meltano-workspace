SELECT
    id,
    siret,
    "name",
    address,
    handlebsdasri as "handle_bsdasri"
FROM
    {{ source("raw_zone_trackdechets", "eco_organisme_raw") }}
