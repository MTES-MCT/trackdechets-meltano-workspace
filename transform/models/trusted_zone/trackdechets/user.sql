SELECT
    id as "id",
    email as "email",
    "name",
    phone as "phone",
    createdat as "created_at",
    updatedat as "updated_at",
    isactive as "is_active",
    activatedat as "activated_at",
    firstassociationdate as "first_association_date",
    isadmin as "is_admin",
    isregistrenational as "is_registre_national"
FROM
    {{ source("raw_zone_trackdechets", "user") }}
