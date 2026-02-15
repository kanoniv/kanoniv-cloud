#!/usr/bin/env python3
"""
Step 1: Initial batch load.

Reads all staged tables from Snowflake, sends them to Kanoniv Cloud
for a full batch reconciliation, and writes golden records + crosswalk
back to Snowflake. Run this once to seed the persistent identity graph.

After this, use incremental_resolve.py for CDC-driven updates.

Usage:
    export SNOWFLAKE_ACCOUNT="your-account.us-east-1"
    export SNOWFLAKE_USER="your_user"
    export SNOWFLAKE_PASSWORD="your_password"
    export KANONIV_API_KEY="kn_..."
    python initial_load.py
"""
import os
import sys
from urllib.parse import quote_plus

import pandas as pd
from kanoniv import Client, Source, Spec
from kanoniv.cloud import reconcile as cloud_reconcile


# -- Configuration ------------------------------------------------------------

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"Error: {name} environment variable is required.", file=sys.stderr)
        sys.exit(1)
    return value


SF_CONN = (
    "snowflake://{user}:{password}@{account}/{database}/{schema}"
    "?warehouse={warehouse}&role={role}"
).format(
    user=_require_env("SNOWFLAKE_USER"),
    password=quote_plus(_require_env("SNOWFLAKE_PASSWORD")),
    account=_require_env("SNOWFLAKE_ACCOUNT"),
    database=os.getenv("SNOWFLAKE_DATABASE", "KANONIV_IDENTITY"),
    schema=os.getenv("SNOWFLAKE_SCHEMA", "STAGING"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
)

API_KEY = os.getenv("KANONIV_API_KEY")
API_TOKEN = os.getenv("KANONIV_ACCESS_TOKEN")
API_URL = os.getenv("KANONIV_API_URL", "https://api.kanoniv.com")

if not API_KEY and not API_TOKEN:
    print("Error: Set KANONIV_API_KEY or KANONIV_ACCESS_TOKEN.", file=sys.stderr)
    sys.exit(1)


# -- Batch Reconcile ----------------------------------------------------------

spec = Spec.from_file("specs/customer.yaml")

sources = [
    Source.from_warehouse("crm_contacts",     "stg_crm_contacts",     SF_CONN, primary_key="external_id"),
    Source.from_warehouse("billing_accounts", "stg_billing_accounts", SF_CONN, primary_key="external_id"),
    Source.from_warehouse("support_users",    "stg_support_users",    SF_CONN, primary_key="external_id"),
]

print("Running initial batch reconcile via Kanoniv Cloud...")

client = Client(base_url=API_URL, api_key=API_KEY, access_token=API_TOKEN)

result = cloud_reconcile(sources, spec, client=client)

print(result.summary())


# -- Write results back to Snowflake ------------------------------------------

raw = result.to_pandas()
print(f"\nFlattening {len(raw)} golden records for Snowflake...")

rows = []
for _, row in raw.iterrows():
    entity = row.get("entity", row)
    if isinstance(entity, dict):
        data = entity.get("canonical_data", {})
        rows.append({
            "kanoniv_id": entity.get("id"),
            "entity_type": entity.get("entity_type"),
            "first_name": data.get("first_name"),
            "last_name": data.get("last_name"),
            "email": data.get("email"),
            "phone": data.get("phone"),
            "company_name": data.get("company_name"),
            "confidence_score": entity.get("confidence_score"),
            "source_name": data.get("source_name"),
            "created_at": entity.get("created_at"),
        })
    else:
        rows.append(row.to_dict())

golden = pd.DataFrame(rows)

# Build crosswalk via bulk endpoint
print("Building entity crosswalk...")
entity_ids = [str(r.get("kanoniv_id", "")) for r in rows if r.get("kanoniv_id")]

crosswalk_rows = []
for i in range(0, len(entity_ids), 1000):
    chunk = entity_ids[i:i + 1000]
    try:
        bulk_resp = client.entities.get_linked_bulk(chunk)
        for kid, linked_list in bulk_resp.get("results", {}).items():
            if linked_list:
                for ext in linked_list:
                    crosswalk_rows.append({
                        "source_system": ext.get("source_name", ""),
                        "source_id": ext.get("external_id", ""),
                        "kanoniv_id": kid,
                    })
            else:
                crosswalk_rows.append({
                    "source_system": "",
                    "source_id": "",
                    "kanoniv_id": kid,
                })
    except Exception as e:
        print(f"  Warning: bulk linked lookup failed: {e}")

crosswalk = pd.DataFrame(crosswalk_rows)

# Write to Snowflake
try:
    import pyarrow as pa
    from kanoniv.cloud_io import write_parquet_to_warehouse

    golden_arrow = pa.Table.from_pandas(golden)
    crosswalk_arrow = pa.Table.from_pandas(crosswalk)
    counts = write_parquet_to_warehouse(
        {"resolved_entities": golden_arrow, "entity_crosswalk": crosswalk_arrow},
        SF_CONN,
        schema="KANONIV_RESOLVED",
    )
    print(f"Done! {counts.get('resolved_entities', 0)} golden records + "
          f"{counts.get('entity_crosswalk', 0)} crosswalk links written via Parquet.")
except ImportError:
    from sqlalchemy import create_engine, text

    engine = create_engine(SF_CONN)
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS KANONIV_RESOLVED"))
        conn.commit()

    golden.to_sql("resolved_entities", engine, schema="KANONIV_RESOLVED",
                  if_exists="replace", index=False)
    crosswalk.to_sql("entity_crosswalk", engine, schema="KANONIV_RESOLVED",
                     if_exists="replace", index=False)
    print(f"Done! {len(golden)} golden records + {len(crosswalk)} crosswalk links written.")

print("\nIdentity graph seeded. Now create the Snowflake Stream:")
print("  CREATE OR REPLACE STREAM identity_input_stream")
print("  ON VIEW staging.stg_identity_input")
print("  APPEND_ONLY = FALSE;")
print("\nThen run: python incremental_resolve.py")
