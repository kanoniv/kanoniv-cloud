-- Support users: parse "FIRSTNAME LASTNAME" display_name into first/last
with source as (
    select * from {{ source('raw', 'support_users') }}
)

select
    support_user_id                         as external_id,
    'support_users'                         as source_system,
    lower(trim(email))                      as email,
    regexp_replace(phone, '[^0-9]', '')     as phone,
    initcap(trim(split_part(display_name, ' ', 1)))
                                            as first_name,
    initcap(trim(
        substr(display_name, position(' ' in display_name) + 1)
    ))                                      as last_name,
    initcap(trim(company))                  as company_name
from source
where email is not null or phone is not null
