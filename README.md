# Kanoniv Cloud Examples

Production-ready examples for [Kanoniv Cloud](https://kanoniv.com) - the managed identity resolution platform.

Each example is a self-contained project that demonstrates a common identity resolution pattern using Kanoniv Cloud with real (synthetic) data.

## Examples

| Example | Description | Sources | Records |
|---------|-------------|---------|---------|
| [customer-identity-resolution](./customer-identity-resolution/) | Unify customers across 5 systems using dbt + Snowflake + Kanoniv Cloud | CRM, Billing, Support, App, Partner | ~6,500 |

## How Kanoniv Cloud Works

Kanoniv Cloud is a managed identity resolution API. You define matching rules in YAML, upload your data, and the cloud engine handles blocking, scoring, clustering, and golden record assembly. Results persist in a live identity graph that you can query at any time.

```
Your warehouse  ->  dbt (normalize)  ->  Kanoniv Cloud (resolve)  ->  Your warehouse (golden records)
```

**What makes it different from running Kanoniv locally:**

- **Persistent identity graph** - Canonical entities, links, and audit history are stored server-side. Query them via API at any time.
- **Real-time resolution** - Resolve `system + external_id` to a canonical identity in a single API call.
- **Run health monitoring** - Every job returns a health assessment with signals and recommendations.
- **Entity lifecycle** - Lock entities, revert to prior states, view full change history, manually override decisions.
- **Audit trail** - Every merge, split, override, and revert is recorded with full provenance.

## Prerequisites

All examples require:

- **Python 3.9+**
- **Kanoniv Cloud API key** - sign up at [app.kanoniv.com](https://app.kanoniv.com)

```bash
pip install kanoniv[cloud]
```

For Snowflake-based examples, also install:

```bash
pip install kanoniv[cloud,dataplane] dbt-snowflake
```

## Links

- [Kanoniv Documentation](https://kanoniv.com/docs)
- [Python SDK](https://pypi.org/project/kanoniv/)
- [Cloud SDK Reference](https://kanoniv.com/docs/sdks/cloud)
- [Arrow Data Plane Guide](https://kanoniv.com/docs/guides/arrow-dataplane)
- [dbt-kanoniv Package](https://github.com/kanoniv/dbt-kanoniv)

## License

MIT
