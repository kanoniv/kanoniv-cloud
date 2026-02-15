-- Partner leads: direct field mapping
with source as (
    select * from {{ source('raw', 'partner_leads') }}
)

select
    partner_lead_id                         as external_id,
    'partner_leads'                         as source_system,
    lower(trim(email))                      as email,
    null                                    as phone,
    initcap(trim(first_name))              as first_name,
    initcap(trim(last_name))               as last_name,
    trim(company)                           as company_name
from source
where email is not null or first_name is not null
