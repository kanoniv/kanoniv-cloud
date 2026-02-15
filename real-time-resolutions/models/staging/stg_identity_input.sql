-- Unified staging view: all sources combined.
-- Create a Snowflake Stream on this view to capture row-level changes:
--
--   CREATE OR REPLACE STREAM identity_input_stream
--   ON VIEW staging.stg_identity_input
--   APPEND_ONLY = FALSE;
--
-- The incremental_resolve.py script reads from the stream to process
-- only changed rows via the Kanoniv real-time resolve API.

select * from {{ ref('stg_crm_contacts') }}
union all
select * from {{ ref('stg_billing_accounts') }}
union all
select * from {{ ref('stg_support_users') }}
