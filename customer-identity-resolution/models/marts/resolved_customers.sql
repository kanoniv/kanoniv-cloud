-- Golden customer records from Kanoniv identity resolution.
-- Each row is one resolved customer with the best canonical fields.
select
    kanoniv_id,
    email,
    phone,
    first_name,
    last_name,
    company_name,
    confidence_score,
    entity_type,
    source_name,
    created_at
from {{ source('kanoniv', 'resolved_entities') }}
