-- CRM contacts: direct field mapping, minimal normalization
with source as (
    select * from {{ source('raw', 'crm_contacts') }}
)

select
    crm_contact_id                          as external_id,
    'crm_contacts'                          as source_system,
    lower(trim(email))                      as email,
    regexp_replace(phone, '[^0-9]', '')     as phone,
    initcap(trim(first_name))              as first_name,
    initcap(trim(last_name))               as last_name,
    trim(company_name)                      as company_name
from source
where email is not null or phone is not null
