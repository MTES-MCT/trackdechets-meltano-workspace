SELECT
    id                   AS "id",
    email                AS "email",
    "name",
    phone                AS "phone",
    createdat            AS "created_at",
    updatedat            AS "updated_at",
    isactive             AS "is_active",
    activatedat          AS "activated_at",
    firstassociationdate AS "first_association_date",
    isadmin              AS "is_admin",
    isregistrenational   AS "is_registre_national"
FROM
    {{ source("raw_zone_trackdechets", "user_raw") }}
