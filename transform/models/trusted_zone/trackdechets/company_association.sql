SELECT
    id,
    "role",
    companyid AS "company_id",
    userid AS "user_id"
FROM
    {{ source("raw_zone_trackdechets", "company_association_raw") }}
