SELECT
    id,
    created_at,
    created_by_id,
    updated_at,
    updated_by_id,
    active,
    verified,
    login,
    email,
    firstname,
    lastname,
    address,
    authorization_ids,
    city,
    country,
    department,
    fax,
    group_ids,
    image,
    image_source,
    karma_user_ids,
    last_login,
    login_failed,
    mobile,
    note,
    organization_id,
    organization_ids,
    out_of_office,
    out_of_office_end_at,
    out_of_office_replacement_id,
    out_of_office_start_at,
    phone,
    role_ids,
    source,
    street,
    vip,
    web,
    zip
FROM
    {{ source("raw_zone_zammad", "users") }}
