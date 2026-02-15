#!/usr/bin/env python3
"""
Test real-time identity resolution against the persistent graph.

Sends new records to POST /v1/resolve/realtime and shows how Kanoniv
handles each case:
  - Record matching an existing entity (by email, phone, name)
  - Completely new record (no match)
  - Duplicate submission (same source+id seen before)

Prerequisites: Run the batch tutorial (customer-identity-resolutions)
first to seed the identity graph.

Usage:
    export KANONIV_API_KEY="kn_..."
    python test_realtime.py
"""
import os
import sys
import json
import time

import requests


API_KEY = os.getenv("KANONIV_API_KEY")
API_URL = os.getenv("KANONIV_API_URL", "https://api.kanoniv.com")

if not API_KEY:
    print("Error: Set KANONIV_API_KEY.", file=sys.stderr)
    sys.exit(1)

headers = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY,
}


def resolve(source_name, external_id, data):
    """Resolve a single record via the real-time API."""
    payload = {
        "source_name": source_name,
        "external_id": external_id,
        "data": {k: v for k, v in data.items() if v is not None},
    }
    resp = requests.post(
        f"{API_URL}/v1/resolve/realtime",
        headers=headers,
        json=payload,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


# -- Test records -------------------------------------------------------------
# These simulate new data arriving after the initial batch reconciliation.

test_cases = [
    {
        "label": "1. Existing CRM email (should match)",
        "description": "Same email as an existing CRM contact -- should find the entity",
        "source_name": "crm_contacts",
        "external_id": "NEW_CRM_001",
        "data": {
            "email": "bob.smith@gmail.com",
            "first_name": "Bob",
            "last_name": "Smith",
            "phone": "5551234567",
        },
    },
    {
        "label": "2. New billing record, email matches support (should match)",
        "description": "Email exists in support system -- should link to same entity",
        "source_name": "billing_accounts",
        "external_id": "NEW_BILL_001",
        "data": {
            "email": "charlie@hotmail.com",
            "first_name": "Charlie",
            "last_name": "Brown",
            "company_name": "Peanuts Inc",
        },
    },
    {
        "label": "3. Completely new person (no match)",
        "description": "No matching email, phone, or name in the graph",
        "source_name": "crm_contacts",
        "external_id": "NEW_CRM_002",
        "data": {
            "email": "newperson@example.com",
            "first_name": "Zara",
            "last_name": "Nguyen",
            "phone": "5550000001",
            "company_name": "FreshCo",
        },
    },
    {
        "label": "4. Another new person (no match)",
        "description": "Completely unknown identity",
        "source_name": "support_users",
        "external_id": "NEW_SUP_001",
        "data": {
            "email": "unknown@nowhere.com",
            "first_name": "Xavier",
            "last_name": "Bloom",
        },
    },
    {
        "label": "5. Same source+id as test 3 (duplicate submission)",
        "description": "Re-submitting NEW_CRM_002 -- should return the same entity_id",
        "source_name": "crm_contacts",
        "external_id": "NEW_CRM_002",
        "data": {
            "email": "newperson@example.com",
            "first_name": "Zara",
            "last_name": "Nguyen",
            "phone": "5550000001",
            "company_name": "FreshCo",
        },
    },
    {
        "label": "6. Name match, different email (should match via name)",
        "description": "Same name as test 3's person but from a different source with different email",
        "source_name": "billing_accounts",
        "external_id": "NEW_BILL_002",
        "data": {
            "email": "zara.nguyen@work.com",
            "first_name": "Zara",
            "last_name": "Nguyen",
            "company_name": "FreshCo",
        },
    },
]


# -- Run tests ----------------------------------------------------------------

print("=" * 70)
print("Real-Time Identity Resolution Test")
print("=" * 70)
print(f"API: {API_URL}")
print(f"Records to resolve: {len(test_cases)}")
print()

results = []

for tc in test_cases:
    print(f"--- {tc['label']} ---")
    print(f"    {tc['description']}")
    print(f"    source={tc['source_name']}  id={tc['external_id']}")
    print(f"    data={json.dumps(tc['data'])}")

    start = time.time()
    try:
        r = resolve(tc["source_name"], tc["external_id"], tc["data"])
        elapsed = (time.time() - start) * 1000

        print(f"    -> entity_id:     {r['entity_id']}")
        print(f"    -> is_new:        {r['is_new']}")
        print(f"    -> matched_source: {r.get('matched_source', '-')}")
        print(f"    -> confidence:    {r.get('confidence', 0):.3f}")
        print(f"    -> latency:       {elapsed:.0f}ms")

        results.append({
            "label": tc["label"],
            "entity_id": r["entity_id"],
            "is_new": r["is_new"],
            "matched_source": r.get("matched_source"),
            "latency_ms": elapsed,
        })
    except Exception as e:
        print(f"    -> ERROR: {e}")
        results.append({"label": tc["label"], "error": str(e)})

    print()

# -- Summary ------------------------------------------------------------------

print("=" * 70)
print("Summary")
print("=" * 70)

new_count = sum(1 for r in results if r.get("is_new"))
matched_count = sum(1 for r in results if not r.get("is_new") and "error" not in r)
error_count = sum(1 for r in results if "error" in r)
avg_latency = sum(r.get("latency_ms", 0) for r in results) / max(len(results), 1)

print(f"  Total records:    {len(results)}")
print(f"  New entities:     {new_count}")
print(f"  Matched existing: {matched_count}")
print(f"  Errors:           {error_count}")
print(f"  Avg latency:      {avg_latency:.0f}ms")
print()

# Check duplicate test (tests 3 and 5 should have same entity_id)
if len(results) >= 5 and "entity_id" in results[2] and "entity_id" in results[4]:
    if results[2]["entity_id"] == results[4]["entity_id"]:
        print("  [PASS] Duplicate submission returned same entity_id")
    else:
        print("  [FAIL] Duplicate submission returned DIFFERENT entity_id!")
        print(f"         Test 3: {results[2]['entity_id']}")
        print(f"         Test 5: {results[4]['entity_id']}")

# Check name match (tests 3 and 6 -- Zara Nguyen from different sources)
if len(results) >= 6 and "entity_id" in results[2] and "entity_id" in results[5]:
    if results[2]["entity_id"] == results[5]["entity_id"]:
        print("  [PASS] Name match across sources linked to same entity")
    else:
        print("  [INFO] Name match created separate entity (may need tuning)")
        print(f"         Test 3: {results[2]['entity_id']}")
        print(f"         Test 6: {results[5]['entity_id']}")
