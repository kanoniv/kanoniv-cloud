-- Billing accounts: parse "Last, First" account_name into first/last
with source as (
    select * from {{ source('raw', 'billing_accounts') }}
)

select
    billing_account_id                      as external_id,
    'billing_accounts'                      as source_system,
    lower(trim(email))                      as email,
    null                                    as phone,
    case
        when account_name like '%,%'
        then initcap(trim(split_part(account_name, ',', 2)))
        else initcap(trim(split_part(account_name, ' ', 1)))
    end                                     as first_name,
    case
        when account_name like '%,%'
        then initcap(trim(split_part(account_name, ',', 1)))
        else initcap(trim(
            substr(account_name, position(' ' in account_name) + 1)
        ))
    end                                     as last_name,
    trim(company_name)                      as company_name
from source
where email is not null
