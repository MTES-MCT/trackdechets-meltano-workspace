with source as (
        select * from {{ source('raw_zone_trackdechets', 'worker_certification_raw') }}
  ),
  renamed as (
      select
        {{ adapter.quote("id") }},
        {{ adapter.quote("hasSubSectionFour") }} as has_sub_section_four,
        {{ adapter.quote("hasSubSectionThree") }} as has_sub_section_three,
        {{ adapter.quote("certificationNumber") }} as certification_number,
        {{ adapter.quote("validityLimit") }} as validity_limit,
        {{ adapter.quote("organisation") }}

      from source
  )
  select * from renamed
    