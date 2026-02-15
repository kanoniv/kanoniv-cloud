-- Maps every source record to its resolved kanoniv_id.
-- Built from the ingested entities stored in Snowflake during reconciliation.
--
-- Example:
--   SELECT i.*, c.kanoniv_id
--   FROM raw.billing_invoices i
--   JOIN resolved.customer_crosswalk c
--     ON c.source_system = 'billing_accounts'
--    AND c.source_id = i.billing_account_id
select
    source_system,
    source_id,
    kanoniv_id
from {{ source('kanoniv', 'entity_crosswalk') }}
