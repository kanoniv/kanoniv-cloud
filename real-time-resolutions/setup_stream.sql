-- Run this in Snowflake after dbt creates the staging views.
-- The stream captures row-level changes (inserts, updates, deletes)
-- on the unified staging view so incremental_resolve.py only
-- processes delta rows.

CREATE OR REPLACE STREAM identity_input_stream
ON VIEW staging.stg_identity_input
APPEND_ONLY = FALSE;

-- Verify the stream is working:
-- SELECT * FROM identity_input_stream LIMIT 10;

-- The stream auto-advances after each SELECT that consumes rows.
-- If incremental_resolve.py reads from it, those rows are "consumed"
-- and won't appear on the next read.
