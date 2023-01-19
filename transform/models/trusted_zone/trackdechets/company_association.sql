SELECT
    id,
    "role",
    companyid as "company_id",
    userid as "user_id"
FROM
    {{ source("raw_zone_trackdechets", "company_association_raw") }}
