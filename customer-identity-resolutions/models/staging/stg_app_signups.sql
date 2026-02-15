-- App signups: direct field mapping
with source as (
    select * from {{ source('raw', 'app_signups') }}
)

select
    app_user_id                             as external_id,
    'app_signups'                           as source_system,
    lower(trim(email))                      as email,
    null                                    as phone,
    initcap(trim(first_name))              as first_name,
    initcap(trim(last_name))               as last_name,
    null                                    as company_name
from source
where email is not null
