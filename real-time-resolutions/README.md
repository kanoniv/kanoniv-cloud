# Incremental Customer Identity Resolution

Real-time identity resolution using Kanoniv Cloud's persistent identity graph.

New records are resolved against the existing graph in ~230ms per record --
no full dataset recompute. Kanoniv handles deduplication, idempotency, and
entity assignment in a single API call.

## Architecture

```
Initial batch reconcile (seed the graph)
      |
      v
New records arrive
      |
      v
POST /v1/resolve/realtime (per record)
      |
      v
Kanoniv Cloud persistent identity graph
  - Redis fast path (sub-ms for known source+id)
  - Field matching (email, phone, name)
  - Ingest + link in one call
      |
      v
Query results via API or export to warehouse
```

## What This Example Demonstrates

The `test_realtime.py` script sends 6 test records to the Kanoniv Cloud
real-time resolve endpoint and shows how each case is handled:

| Test | Record | Expected | Result |
|------|--------|----------|--------|
| 1 | CRM contact with known email | Match existing entity | New entity (email variant) |
| 2 | Billing record, email in support | Match via email | New entity (email variant) |
| 3 | Completely new person | Create new entity | New entity |
| 4 | Another new person | Create new entity | New entity |
| 5 | Re-submit test 3 (same source+id) | Return same entity | Same entity_id (idempotent) |
| 6 | Same name as test 3, different email | May match via name | Separate entity |

**Key findings from the test run:**

- **Idempotency works**: re-submitting the same `source+id` returns the existing
  entity (`is_new: false`) via Redis fast-path lookup (~161ms)
- **Average latency**: ~229ms per record
- **Field matching**: exact email/phone matches link to existing entities.
  Name-only matching across sources requires the full batch reconcile
  (Fellegi-Sunter probabilistic scoring) rather than real-time resolve.

## Prerequisites

- Python 3.9+
- [Kanoniv Cloud API key](https://app.kanoniv.com)
- An existing identity graph (run the [batch example](../customer-identity-resolutions/) first)

```bash
pip install kanoniv[cloud] requests
```

For the full CDC pipeline with Snowflake Streams, also install:

```bash
pip install kanoniv[cloud,dataplane] dbt-snowflake snowflake-connector-python
```

## Quick Start

### 1. Set your API key

```bash
export KANONIV_API_KEY="kn_..."
```

### 2. Run the real-time resolve test

```bash
python test_realtime.py
```

Expected output:

```
======================================================================
Real-Time Identity Resolution Test
======================================================================
API: https://api.kanoniv.com
Records to resolve: 6

--- 1. Existing CRM email (should match) ---
    Same email as an existing CRM contact -- should find the entity
    source=crm_contacts  id=NEW_CRM_001
    -> entity_id:     f1c7c99c-7d9f-44da-bebe-a393fa66cb82
    -> is_new:        True
    -> matched_source: None
    -> confidence:    1.000
    -> latency:       292ms

--- 3. Completely new person (no match) ---
    No matching email, phone, or name in the graph
    source=crm_contacts  id=NEW_CRM_002
    -> entity_id:     97d64465-5edc-49a1-bb52-901363098413
    -> is_new:        True
    -> confidence:    1.000
    -> latency:       169ms

--- 5. Same source+id as test 3 (duplicate submission) ---
    Re-submitting NEW_CRM_002 -- should return the same entity_id
    source=crm_contacts  id=NEW_CRM_002
    -> entity_id:     97d64465-5edc-49a1-bb52-901363098413
    -> is_new:        False
    -> matched_source: crm_contacts
    -> confidence:    1.000
    -> latency:       161ms

======================================================================
Summary
======================================================================
  Total records:    6
  New entities:     5
  Matched existing: 1
  Errors:           0
  Avg latency:      229ms

  [PASS] Duplicate submission returned same entity_id
```

### 3. (Optional) Full CDC pipeline with Snowflake Streams

If you want to run the full CDC pipeline with Snowflake:

```bash
export SNOWFLAKE_ACCOUNT="your-account.us-east-1"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
```

**a. Seed data and build staging views:**

```bash
dbt deps
dbt seed
dbt run --select staging
```

**b. Create the Snowflake Stream:**

```sql
CREATE OR REPLACE STREAM identity_input_stream
ON VIEW staging.stg_identity_input
APPEND_ONLY = FALSE;
```

**c. Insert new data and resolve incrementally:**

```sql
INSERT INTO raw.crm_contacts
  (crm_contact_id, email, first_name, last_name, phone, company_name, created_at)
VALUES
  ('C009', 'bob@work.com', 'Bob', 'Smith', '5551234567', 'Acme Corp', CURRENT_TIMESTAMP());
```

```bash
dbt run --select staging
python incremental_resolve.py
dbt run --select marts
```

The script reads only changed rows from the Snowflake Stream, resolves each
via `POST /v1/resolve/realtime`, and MERGEs results back into Snowflake.

## How POST /v1/resolve/realtime Works

Each call resolves a single record against the persistent identity graph:

```
POST /v1/resolve/realtime
{
  "source_name": "crm_contacts",
  "external_id": "NEW_CRM_002",
  "data": {
    "email": "newperson@example.com",
    "first_name": "Zara",
    "last_name": "Nguyen",
    "phone": "5550000001"
  }
}
```

Response:

```json
{
  "entity_id": "97d64465-5edc-49a1-bb52-901363098413",
  "is_new": true,
  "matched_source": null,
  "confidence": 1.0,
  "canonical_data": { ... }
}
```

**Under the hood:**

1. **Redis fast path** - checks `kv:{tenant}:{source}:{id}` in Redis.
   If this source+id was seen before, returns the existing entity immediately.

2. **Field matching** - if the source+id is new, searches existing entities
   by email and phone (exact match). If a match is found, links to that entity.

3. **Ingest + link** - the record is ingested into the persistent graph.
   If a match was found, it's linked to the existing canonical entity.
   If not, a new canonical entity is created.

4. **Redis populated** - the reverse index is updated so future lookups
   for this source+id are sub-millisecond.

All of this happens in a single API call. No job queues, no batch windows.

## Production Scheduling

Schedule `incremental_resolve.py` to run on your preferred cadence:

**Cron (every 5 minutes):**

```cron
*/5 * * * * cd /path/to/real-time-resolutions && python incremental_resolve.py >> /var/log/kanoniv-resolve.log 2>&1
```

**Airflow:**

```python
resolve_task = BashOperator(
    task_id="kanoniv_incremental_resolve",
    bash_command="cd /path/to/real-time-resolutions && python incremental_resolve.py",
    dag=dag,
)
```

## Comparison: Batch vs Real-Time

| | Batch (customer-identity-resolutions) | Real-Time (`POST /v1/resolve/realtime`) |
|---|---|---|
| **API** | `cloud_reconcile()` (full job) | `POST /v1/resolve/realtime` (per record) |
| **Data** | All records | Single record |
| **Latency** | Minutes (depends on volume) | ~230ms per record |
| **Matching** | Full Fellegi-Sunter probabilistic | Exact field match (email, phone) |
| **Recompute** | Full blocking + scoring + clustering | Lookup + match against existing graph |
| **When** | Initial seed, periodic full refresh | Continuous / on every new record |

## File Structure

```
real-time-resolutions/
  README.md                   # This file
  test_realtime.py            # Test script: sends 6 records, shows results
  incremental_resolve.py      # CDC pipeline: Snowflake Stream -> resolve -> MERGE
  initial_load.py             # Batch seed (uses cloud_reconcile)
  setup_stream.sql            # Snowflake Stream DDL
  specs/customer.yaml         # Kanoniv identity resolution spec
  dbt_project.yml             # dbt project config
  packages.yml                # dbt-kanoniv package
  profiles.yml.example        # Snowflake connection template
  macros/
    generate_schema_name.sql  # Custom schema routing
  models/
    staging/                  # Normalize raw data
    marts/                    # Customer 360 view
  data/                       # Sample seed CSVs (20 records)
```

## Links

- [Kanoniv Documentation](https://kanoniv.com/docs)
- [Cloud SDK Reference](https://kanoniv.com/docs/sdks/cloud)
- [Arrow Data Plane Guide](https://kanoniv.com/docs/guides/arrow-dataplane)
- [Batch Example](../customer-identity-resolutions/)
