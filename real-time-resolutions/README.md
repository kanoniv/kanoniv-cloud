# Incremental Customer Identity Resolution

Real-time identity resolution using Kanoniv Cloud's persistent identity graph.

New records are resolved against the existing graph in ~188ms per record -
no full dataset recompute. Kanoniv handles deduplication, idempotency, and
entity assignment in a single API call.

As of February 2026, the resolve endpoint uses **Fellegi-Sunter probabilistic
scoring** - the same matching model used in full batch reconciliation - rather
than simple exact field matching. This means real-time resolves benefit from
trained m/u parameters, composite blocking keys, and configurable thresholds.

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
  - Blocking key retrieval (email, phone, name composites)
  - Fellegi-Sunter probabilistic scoring
  - Two-threshold decision: link vs merge
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
| 1 | CRM contact with known email | Match existing entity | Matched (FS score above threshold) |
| 2 | Billing record, email in support | Match via email | Matched (FS score above threshold) |
| 3 | Completely new person | Create new entity | New entity |
| 4 | Another new person | Create new entity | New entity |
| 5 | Re-submit test 3 (same source+id) | Return same entity | Same entity_id (idempotent) |
| 6 | Same name as test 3, different email | Separate entity (conservative) | Separate entity (below match threshold) |

**Test results: 6/6 pass, 0 errors, 188ms avg latency.**

**Key findings from the test run:**

- **Fellegi-Sunter scoring**: the resolve endpoint now uses probabilistic
  scoring with trained m/u parameters, matching the same model used in batch
  reconciliation. Email and phone carry high discriminative weight; name-only
  overlap with a different email scores below the match threshold.
- **Idempotency works**: re-submitting the same `source+id` returns the existing
  entity (`is_new: false`) via Redis fast-path lookup.
- **Average latency**: ~188ms per record.
- **Conservative on name-only**: test 6 (same name, different email) correctly
  creates a separate entity rather than a false merge. This is the expected
  behavior - name-only evidence is not strong enough to exceed the match
  threshold on its own.

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

Expected output (abbreviated):

```
======================================================================
Real-Time Identity Resolution Test
======================================================================
API: https://api.kanoniv.com
Records to resolve: 6

--- 1. Existing CRM email (should match) ---
    source=crm_contacts  id=NEW_CRM_001
    -> entity_id:     <matched entity>
    -> is_new:        False
    -> matched_source: crm_contacts
    -> confidence:    0.952
    -> latency:       210ms

--- 3. Completely new person (no match) ---
    source=crm_contacts  id=NEW_CRM_002
    -> entity_id:     <new entity>
    -> is_new:        True
    -> confidence:    1.000
    -> latency:       169ms

--- 5. Same source+id as test 3 (duplicate submission) ---
    source=crm_contacts  id=NEW_CRM_002
    -> entity_id:     <same as test 3>
    -> is_new:        False
    -> matched_source: crm_contacts
    -> confidence:    1.000
    -> latency:       145ms

--- 6. Name match, different email (should be separate) ---
    source=billing_accounts  id=NEW_BILL_002
    -> entity_id:     <new entity - different from test 3>
    -> is_new:        True
    -> confidence:    1.000
    -> latency:       182ms

======================================================================
Summary
======================================================================
  Total records:    6
  New entities:     4
  Matched existing: 2
  Errors:           0
  Avg latency:      188ms

  [PASS] Duplicate submission returned same entity_id
  [INFO] Name match created separate entity (below match threshold)
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

2. **Blocking key retrieval** - generates blocking keys from the incoming
   record's fields: normalized email, normalized phone, and name composites
   (first+last). These keys are used to retrieve a candidate set from the
   identity graph without scanning every entity.

3. **Fellegi-Sunter scoring** - each candidate is scored using the same
   probabilistic model as batch reconciliation. Trained m/u parameters
   (match and unmatch frequencies) determine how much weight each field
   agreement or disagreement carries. Email and phone agreement contribute
   high weight; name-only agreement contributes moderate weight.

4. **Two-threshold decision** - the composite FS score is compared against
   two thresholds:
   - `match_threshold` - if the score exceeds this, the record is **linked**
     to the existing entity (same canonical entity, new membership).
   - `merge_threshold` - if the score exceeds this higher bar, the record's
     entity may be **merged** with an existing entity (two previously separate
     entities become one).

   Scores below `match_threshold` result in a new entity. This is why
   name-only matches (test 6) correctly create a separate entity - the
   FS score is below the match threshold.

5. **Ingest + link** - the record is ingested into the persistent graph.
   If a match was found, it is linked to the existing canonical entity.
   If not, a new canonical entity is created.

6. **Redis populated** - the reverse index is updated so future lookups
   for this source+id are sub-millisecond.

All of this happens in a single API call. No job queues, no batch windows.

## Enhanced Incremental Reconciliation (Fellegi-Sunter Scoring)

Prior to this update, the real-time resolve endpoint used exact field matching
only - email and phone had to match character-for-character to link records.
Name-only matching was not supported and required a full batch reconcile.

The endpoint now uses **Fellegi-Sunter probabilistic scoring**, the same
matching model that powers batch reconciliation. This brings several
improvements to real-time resolution:

### Blocking Keys for Candidate Retrieval

Instead of scanning all entities, the endpoint generates blocking keys from
the incoming record to narrow the candidate set:

- **Email blocking key** - normalized email (lowercased, trimmed)
- **Phone blocking key** - normalized phone (digits only)
- **Name composite key** - first 3 chars of first name + full last name
  (lowercased, trimmed)

Only entities sharing at least one blocking key are scored. This keeps
latency low while expanding the match surface beyond exact email/phone.

### Fellegi-Sunter Probabilistic Scoring

Each candidate retrieved via blocking keys is scored using the trained
Fellegi-Sunter model:

- **m-parameters** (match frequencies) and **u-parameters** (unmatch
  frequencies) are trained during batch reconciliation and persisted.
- Each field comparison produces an agreement or disagreement weight.
  Email agreement carries high weight (~8.5 log-odds). Phone agreement
  is similarly high. Name agreement contributes moderate weight (~3.2
  log-odds). Disagreement on email when it was expected to match
  contributes negative weight.
- The composite score is the sum of individual field weights.

### Two-Threshold Decision Logic

The composite FS score is evaluated against two thresholds:

| Threshold | Purpose | Behavior |
|-----------|---------|----------|
| `match_threshold` | Link to existing entity | Score >= threshold -> record is linked as a new membership of the matched entity |
| `merge_threshold` | Merge two entities | Score >= threshold (higher bar) -> if the incoming record would create an entity that overlaps with an existing one, the entities are merged |

If the score falls below `match_threshold`, a new entity is created. This
is the conservative default - it avoids false merges at the cost of
occasionally under-linking records that share only weak signals (e.g.,
name-only overlap with different email and no phone).

### Test Results

All 6 test cases pass with the enhanced scoring:

| Test | Scenario | FS Behavior | Result |
|------|----------|-------------|--------|
| 1 | Known email | High FS score (email match) | Linked to existing entity |
| 2 | Email in another source | High FS score (email match) | Linked to existing entity |
| 3 | Completely new person | No candidates above threshold | New entity created |
| 4 | Another new person | No candidates above threshold | New entity created |
| 5 | Duplicate source+id | Redis fast path (no FS needed) | Same entity_id (idempotent) |
| 6 | Same name, different email | FS score below match threshold | Separate entity (conservative) |

- **6/6 pass, 0 errors**
- **188ms average latency** (down from 229ms with the old exact-match path)
- **Idempotent duplicates** work correctly via Redis fast path
- **Name-only match** correctly creates a separate entity - the FS score
  for name agreement alone (~3.2 log-odds) does not reach the match
  threshold, preventing false merges

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
| **Latency** | Minutes (depends on volume) | ~188ms per record |
| **Matching** | Fellegi-Sunter probabilistic (full pairwise) | Fellegi-Sunter probabilistic (blocking key candidates) |
| **Blocking** | All configured blocking rules | Email, phone, name composite keys |
| **Thresholds** | Single link threshold + clustering | Two thresholds: match (link) and merge |
| **Recompute** | Full blocking + scoring + clustering | Blocking key retrieval + FS scoring against graph |
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
