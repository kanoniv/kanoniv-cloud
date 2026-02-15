-- Maps every source record to its resolved kanoniv_id.
-- Used to join any source table back to the canonical customer.
--
-- Example:
--   SELECT i.*, c.kanoniv_id
--   FROM raw.billing_accounts i
--   JOIN resolved.customer_crosswalk c
--     ON c.source_system = 'billing_accounts'
--    AND c.source_id = i.billing_account_id
select
    source_system,
    source_id,
    kanoniv_id
from {{ source('kanoniv', 'entity_crosswalk') }}
