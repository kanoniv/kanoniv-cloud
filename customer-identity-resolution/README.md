# Customer Identity Resolution with Kanoniv Cloud

Resolve ~6,500 customer records across 5 source systems into ~2,000 unified golden customers using dbt, Snowflake, and the Kanoniv Cloud API.

## The Problem

Your customers exist in multiple systems, each with its own IDs, name formats, and data quality:

| System | Example Record | Records |
|--------|---------------|---------|
| **CRM** (Salesforce) | Betty Baker, betty.baker4@aol.com, 650-650-6176 | 1,839 |
| **Billing** (Stripe) | "Cook, Victoria", victoria.cook86@icloud.com | 1,200 |
| **Support** (Zendesk) | CHRISTINA BROWN, 617-689-9311 | 1,400 |
| **App** (Product) | raymond.howard79@icloud.com, HOWARD | 1,300 |
| **Partner** (Referrals) | Watson, george.watson1@outlook.com, Amber Group | 800 |

"Bob Smith" in CRM is "Robert Smith" in Billing is "bob.smith+work@gmail.com" in App signups. You need a single customer view.

## The Solution

```
dbt run --select staging    # Normalize 5 source tables in SQL
python reconcile.py         # Kanoniv Cloud resolves identities
dbt run --select marts      # Build Customer 360 views in Snowflake
```

Three steps, three layers of responsibility:

1. **Staging (dbt)** - Analytics engineers write SQL to normalize names, emails, and phones into a consistent schema. Each staging model is ~15 lines.

2. **Reconciliation (Kanoniv Cloud)** - One `python reconcile.py` command uploads the staged data, runs Fellegi-Sunter probabilistic matching with EM training, clusters identities, applies survivorship rules, and writes golden records back to Snowflake.

3. **Marts (dbt)** - SQL models consume the resolved entities to build `resolved_customers`, `customer_crosswalk`, and `customer_360` views.

## What Kanoniv Cloud Does

The `reconcile.py` script calls the Kanoniv Cloud API, which runs a full identity resolution pipeline:

1. **Block** - Groups records by email, phone, name, and company to avoid comparing every pair (O(n^2) down to O(n))
2. **Compare** - Applies exact email/phone matching and fuzzy Jaro-Winkler name/company matching to candidate pairs
3. **Score** - Fellegi-Sunter probabilistic scoring with EM-trained m/u probability estimates
4. **Decide** - Pairs above 0.9 are auto-matched, 0.7-0.9 go to review, below 0.7 are rejected
5. **Cluster** - Connected components form identity clusters
6. **Survive** - CRM fields win by default; email and name use most-complete; phone prefers CRM then Support

After reconciliation, the identity graph persists on Kanoniv Cloud. You can:

- **Resolve** any source ID to its canonical identity via API (`client.resolve()`)
- **Search** the identity graph by email, name, or free text
- **View history** of every merge, split, and data change
- **Override** decisions manually (force-merge or force-split)
- **Export** canonical entities and crosswalk incrementally

## Project Structure

```
customer-identity-resolution/
  data/                          # 10 synthetic CSV files (~6,500 identity records)
    crm_contacts.csv             #   1,839 CRM contacts
    billing_accounts.csv         #   1,200 billing accounts
    support_users.csv            #   1,400 support users
    app_signups.csv              #   1,300 app signups
    partner_leads.csv            #     800 partner leads
    crm_companies.csv            #   enrichment: 107 companies
    billing_invoices.csv         #   enrichment: 2,974 invoices
    support_tickets.csv          #   enrichment: 2,861 tickets
    app_events.csv               #   enrichment: 6,397 events
    partner_referrals.csv        #   enrichment: 484 referrals
  specs/
    kanoniv.yml                  # Identity resolution spec (matching rules, scoring, survivorship)
  models/
    staging/
      sources.yml                # dbt source definitions (raw tables)
      schema.yml                 # dbt model tests
      stg_crm_contacts.sql       # Normalize CRM fields
      stg_billing_accounts.sql   # Parse "Last, First" account names
      stg_support_users.sql      # Parse "FIRSTNAME LASTNAME" display names
      stg_app_signups.sql        # Normalize app signup emails
      stg_partner_leads.sql      # Normalize partner lead fields
    marts/
      sources.yml                # dbt source for Kanoniv resolved tables
      resolved_customers.sql     # Golden customer records
      customer_crosswalk.sql     # source_id -> kanoniv_id mapping
      customer_360.sql           # Enriched view with source coverage
  macros/
    generate_schema_name.sql     # Use custom schema names directly
  reconcile.py                   # Python script: upload, reconcile, write back
  dbt_project.yml                # dbt project config
  packages.yml                   # dbt-kanoniv package dependency
  profiles.yml.example           # Example Snowflake connection (copy to profiles.yml)
```

## Setup

### 1. Prerequisites

- Snowflake account with a warehouse and database you can write to
- dbt Core 1.3+ with the Snowflake adapter
- Python 3.9+
- Kanoniv Cloud API key ([sign up at app.kanoniv.com](https://app.kanoniv.com))

### 2. Install dependencies

```bash
# Core: Kanoniv Cloud SDK + dbt
pip install kanoniv[cloud] dbt-snowflake

# Optional: Arrow data plane for ~6x faster ingest (recommended)
pip install kanoniv[cloud,dataplane]
```

### 3. Configure Snowflake

Copy the example profile and fill in your Snowflake credentials:

```bash
cp profiles.yml.example profiles.yml
```

Edit `profiles.yml` with your Snowflake account, user, and warehouse. Or set environment variables:

```bash
export SNOWFLAKE_ACCOUNT="your-account.us-east-1"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_DATABASE="KANONIV_IDENTITY"    # or any database you own
```

### 4. Configure Kanoniv Cloud

```bash
export KANONIV_API_KEY="kn_your_api_key"
```

### 5. Run the pipeline

```bash
# Install the dbt-kanoniv normalization package
dbt deps

# Load the 10 CSV files into Snowflake as raw tables
dbt seed

# Build staging views (normalize names, emails, phones)
dbt run --select staging

# Reconcile identities via Kanoniv Cloud
python reconcile.py

# Build mart tables (golden records, crosswalk, Customer 360)
dbt run --select marts

# Run data quality tests
dbt test
```

### Expected Output

```
Uploading staged data to Kanoniv Cloud...
Cloud Reconciliation - job a1b2c3d4-...
  Status:          completed
  Duration:        ~120s
  Health:          healthy
  Input entities:  6,443
  Canonicals:      ~2,100
  Merge rate:      ~67%

Writing 2,100 golden records to Snowflake...
Done! Run: dbt run --select marts
```

## Query the Results

Once the pipeline completes, query the resolved data in Snowflake:

```sql
-- How many unique customers?
SELECT count(*) FROM resolved.resolved_customers;

-- Customers appearing in 3+ systems
SELECT * FROM resolved.customer_360
WHERE source_count >= 3
ORDER BY source_count DESC;

-- Join invoices to resolved customers
SELECT
    c.kanoniv_id,
    c.first_name,
    c.last_name,
    sum(i.amount_cents) / 100.0 AS total_invoiced
FROM raw.billing_invoices i
JOIN resolved.customer_crosswalk x
  ON x.source_system = 'billing_accounts'
 AND x.source_id = i.billing_account_id
JOIN resolved.resolved_customers c
  ON c.kanoniv_id = x.kanoniv_id
GROUP BY 1, 2, 3
ORDER BY total_invoiced DESC
LIMIT 20;
```

## Query the Live Identity Graph

After reconciliation, the identity graph persists on Kanoniv Cloud. Query it from any service:

```python
from kanoniv import Client

with Client(api_key="kn_...") as client:
    # Resolve a CRM contact to its canonical identity
    result = client.resolve(system="crm_contacts", external_id="CRM_000042")
    print(f"Canonical ID: {result['canonical_id']}")

    # See every linked record across all 5 systems
    linked = client.entities.get_linked(result["canonical_id"])
    for record in linked["linked"]:
        print(f"  [{record['source_name']}] {record['external_id']}")
    # [crm_contacts] CRM_000042
    # [billing_accounts] BILL_000019
    # [support_users] SUP_000105

    # Search by email
    results = client.entities.search(q="betty.baker4@aol.com")
    for entity in results["data"]:
        print(entity["id"], entity["data"]["email"])
```

## Arrow Data Plane (Optional)

If you installed `kanoniv[cloud,dataplane]`, the `reconcile.py` script automatically uses the Arrow fast path:

- **Reads** Snowflake tables via columnar Arrow fetch instead of row-by-row cursors
- **Stages** all sources in DuckDB with zero-copy UNION ALL
- **Uploads** Parquet files instead of 500-record JSON batches
- **Writes back** via Parquet PUT + COPY INTO instead of pandas `to_sql()`

This is ~6x faster for the full pipeline (~120s vs ~780s). No code changes required - the SDK detects the dataplane extras and warehouse sources automatically.

See the [Arrow Data Plane guide](https://kanoniv.com/docs/guides/arrow-dataplane) for details.

## The Kanoniv Spec

The identity resolution logic lives in [`specs/kanoniv.yml`](./specs/kanoniv.yml). Key design decisions:

**Blocking** - Four composite keys reduce the comparison space:
- `[email]` - Exact email matches
- `[phone]` - Exact phone matches
- `[last_name, first_name]` - Name-based blocking
- `[company_name, last_name]` - Company + last name

**Scoring** - Fellegi-Sunter probabilistic matching with EM-trained weights:
- Email exact match (weight 2.0, m=0.95, u=0.001) with email normalization
- Phone exact match (weight 1.5, m=0.85, u=0.005) with phone normalization
- First name Jaro-Winkler (weight 1.0, m=0.85, u=0.05) with nickname normalization
- Last name Jaro-Winkler (weight 1.0, m=0.88, u=0.02) with name normalization
- Company name Jaro-Winkler (weight 1.0, m=0.80, u=0.02) with generic normalization

**Survivorship** - Source priority with field-level overrides:
- Default: CRM wins (highest data quality)
- Email, first name, last name: most complete value wins
- Phone: CRM first, then Support (the two sources that capture phone)
- Company: CRM, then Billing, then Partner

## Adapting This Example

To use this with your own data:

1. Replace the CSVs in `data/` with your source data (or point dbt at your existing warehouse tables)
2. Edit the staging models to map your source columns to the canonical schema (`email`, `phone`, `first_name`, `last_name`, `company_name`)
3. Edit `specs/kanoniv.yml` to adjust matching rules, weights, and survivorship for your domain
4. Run the pipeline

The matching rules and survivorship strategy in `kanoniv.yml` are designed for B2B customer data. For other domains (healthcare, financial services, e-commerce), you'll want to adjust the fields, comparators, and weights. See the [Spec Reference](https://kanoniv.com/docs/spec-reference/) for all available options.

## Links

- [Kanoniv Cloud SDK Reference](https://kanoniv.com/docs/sdks/cloud)
- [Cloud Tutorial: Snowflake + dbt](https://kanoniv.com/docs/guides/cloud-tutorial)
- [Arrow Data Plane Guide](https://kanoniv.com/docs/guides/arrow-dataplane)
- [Active Learning Guide](https://kanoniv.com/docs/guides/active-learning)
- [Spec Reference](https://kanoniv.com/docs/spec-reference/)
- [dbt-kanoniv Package](https://github.com/kanoniv/dbt-kanoniv)
