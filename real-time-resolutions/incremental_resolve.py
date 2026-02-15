#!/usr/bin/env python3
"""
Step 2: Incremental real-time resolution via Snowflake Streams + Kanoniv Cloud.

Reads changed rows from a Snowflake Stream (CDC), resolves each record
against the persistent identity graph using the Kanoniv real-time API,
and writes updated golden records back to Snowflake.

No full recompute. Each record is resolved in milliseconds against the
existing graph via Redis fast-path lookup and field matching.

Prerequisites:
    1. Run initial_load.py first to seed the identity graph.
    2. Create the Snowflake Stream:

       CREATE OR REPLACE STREAM identity_input_stream
       ON VIEW staging.stg_identity_input
       APPEND_ONLY = FALSE;

    3. Run dbt to refresh staging views when source data changes:

       dbt run --select staging

Usage:
    export SNOWFLAKE_ACCOUNT="your-account.us-east-1"
    export SNOWFLAKE_USER="your_user"
    export SNOWFLAKE_PASSWORD="your_password"
    export KANONIV_API_KEY="kn_..."
    python incremental_resolve.py

Schedule this script via cron, Airflow, or Dagster to run every 5 minutes
(or on any cadence that suits your SLA).
"""
import os
import sys
import json
import time
from urllib.parse import quote_plus

import requests
import snowflake.connector
import pandas as pd


# -- Configuration ------------------------------------------------------------

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"Error: {name} environment variable is required.", file=sys.stderr)
        sys.exit(1)
    return value


SF_ACCOUNT = _require_env("SNOWFLAKE_ACCOUNT")
SF_USER = _require_env("SNOWFLAKE_USER")
SF_PASSWORD = _require_env("SNOWFLAKE_PASSWORD")
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "KANONIV_IDENTITY")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

API_KEY = os.getenv("KANONIV_API_KEY")
API_TOKEN = os.getenv("KANONIV_ACCESS_TOKEN")
API_URL = os.getenv("KANONIV_API_URL", "https://api.kanoniv.com")

if not API_KEY and not API_TOKEN:
    print("Error: Set KANONIV_API_KEY or KANONIV_ACCESS_TOKEN.", file=sys.stderr)
    sys.exit(1)


# -- Snowflake connection -----------------------------------------------------

conn = snowflake.connector.connect(
    account=SF_ACCOUNT,
    user=SF_USER,
    password=SF_PASSWORD,
    database=SF_DATABASE,
    warehouse=SF_WAREHOUSE,
    role=SF_ROLE,
)
cursor = conn.cursor()


# -- Read changed rows from Snowflake Stream ----------------------------------

print("Reading changes from identity_input_stream...")

cursor.execute("""
    SELECT
        source_system,
        external_id,
        email,
        phone,
        first_name,
        last_name,
        company_name,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM staging.identity_input_stream
""")

rows = cursor.fetchall()
columns = [desc[0].lower() for desc in cursor.description]
changes = pd.DataFrame(rows, columns=columns)

if changes.empty:
    print("No changes detected. Identity graph is up to date.")
    conn.close()
    sys.exit(0)

# Filter to inserts and updates (skip deletes for now)
inserts = changes[changes["metadata$action"] == "INSERT"]
print(f"Found {len(changes)} stream rows ({len(inserts)} inserts/updates)")


# -- Resolve each changed record via Kanoniv real-time API --------------------

headers = {"Content-Type": "application/json"}
if API_KEY:
    headers["Authorization"] = f"Bearer {API_KEY}"
elif API_TOKEN:
    headers["Authorization"] = f"Bearer {API_TOKEN}"

resolved = []
errors = []
start = time.time()

for _, row in inserts.iterrows():
    payload = {
        "source_name": row["source_system"],
        "external_id": str(row["external_id"]),
        "data": {
            "email": row.get("email"),
            "phone": row.get("phone"),
            "first_name": row.get("first_name"),
            "last_name": row.get("last_name"),
            "company_name": row.get("company_name"),
        },
    }

    # Remove None values from data
    payload["data"] = {k: v for k, v in payload["data"].items() if v is not None and str(v) != "nan"}

    try:
        resp = requests.post(
            f"{API_URL}/v1/resolve/realtime",
            headers=headers,
            json=payload,
            timeout=10,
        )
        resp.raise_for_status()
        result = resp.json()
        resolved.append({
            "source_system": row["source_system"],
            "external_id": str(row["external_id"]),
            "entity_id": result["entity_id"],
            "is_new": result["is_new"],
            "matched_source": result.get("matched_source"),
            "confidence": result.get("confidence", 0),
            "canonical_data": result.get("canonical_data", {}),
        })
    except Exception as e:
        errors.append({
            "source_system": row["source_system"],
            "external_id": str(row["external_id"]),
            "error": str(e),
        })

elapsed = time.time() - start

print(f"\nResolved {len(resolved)} records in {elapsed:.1f}s "
      f"({elapsed / max(len(resolved), 1) * 1000:.0f}ms/record)")

new_entities = sum(1 for r in resolved if r["is_new"])
matched = len(resolved) - new_entities
print(f"  New entities: {new_entities}")
print(f"  Matched to existing: {matched}")

if errors:
    print(f"  Errors: {len(errors)}")
    for e in errors[:5]:
        print(f"    {e['source_system']}:{e['external_id']} - {e['error']}")


# -- Update Snowflake tables with incremental results -------------------------

if not resolved:
    print("No records resolved. Exiting.")
    conn.close()
    sys.exit(0)

# Update crosswalk with new/changed mappings
print("\nUpdating crosswalk in Snowflake...")
cursor.execute("CREATE SCHEMA IF NOT EXISTS KANONIV_RESOLVED")

crosswalk_values = []
for r in resolved:
    src = r["source_system"].replace("'", "''")
    eid = str(r["external_id"]).replace("'", "''")
    kid = str(r["entity_id"]).replace("'", "''")
    crosswalk_values.append(f"('{src}', '{eid}', '{kid}')")

# Merge into crosswalk (upsert)
cursor.execute(f"""
    MERGE INTO kanoniv_resolved.entity_crosswalk AS target
    USING (
        SELECT
            column1 AS source_system,
            column2 AS source_id,
            column3 AS kanoniv_id
        FROM VALUES {', '.join(crosswalk_values)}
    ) AS source
    ON target.source_system = source.source_system
       AND target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        kanoniv_id = source.kanoniv_id
    WHEN NOT MATCHED THEN INSERT (source_system, source_id, kanoniv_id)
        VALUES (source.source_system, source.source_id, source.kanoniv_id)
""")

# Update resolved entities for any entity IDs that changed
entity_ids = list({r["entity_id"] for r in resolved})
print(f"Refreshing {len(entity_ids)} canonical entities...")

entity_values = []
for r in resolved:
    cd = r.get("canonical_data", {})
    entity_values.append(f"""(
        '{r["entity_id"]}',
        '{cd.get("email", "").replace("'", "''")}',
        '{cd.get("phone", "").replace("'", "''")}',
        '{cd.get("first_name", "").replace("'", "''")}',
        '{cd.get("last_name", "").replace("'", "''")}',
        '{cd.get("company_name", "").replace("'", "''")}',
        {r.get("confidence", 0)},
        'customer',
        '{r["source_system"].replace("'", "''")}',
        CURRENT_TIMESTAMP()
    )""")

cursor.execute(f"""
    MERGE INTO kanoniv_resolved.resolved_entities AS target
    USING (
        SELECT
            column1 AS kanoniv_id,
            column2 AS email,
            column3 AS phone,
            column4 AS first_name,
            column5 AS last_name,
            column6 AS company_name,
            column7 AS confidence_score,
            column8 AS entity_type,
            column9 AS source_name,
            column10 AS created_at
        FROM VALUES {', '.join(entity_values)}
    ) AS source
    ON target.kanoniv_id = source.kanoniv_id
    WHEN MATCHED THEN UPDATE SET
        email = source.email,
        phone = source.phone,
        first_name = source.first_name,
        last_name = source.last_name,
        company_name = source.company_name,
        confidence_score = source.confidence_score
    WHEN NOT MATCHED THEN INSERT
        (kanoniv_id, email, phone, first_name, last_name, company_name,
         confidence_score, entity_type, source_name, created_at)
        VALUES
        (source.kanoniv_id, source.email, source.phone, source.first_name,
         source.last_name, source.company_name, source.confidence_score,
         source.entity_type, source.source_name, source.created_at)
""")

conn.close()

print(f"\nDone! {len(resolved)} records incrementally resolved.")
print("Run: dbt run --select marts")
