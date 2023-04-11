SELECT
    replace(code, ' ','') as code,
    description
FROM
    {{ source('raw_zone', 'codes_operations_traitements') }}
