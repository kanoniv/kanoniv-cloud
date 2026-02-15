-- Customer 360: golden record enriched with source coverage metrics.
-- Shows which systems each customer appears in.
with crosswalk as (
    select * from {{ ref('customer_crosswalk') }}
),

customers as (
    select * from {{ ref('resolved_customers') }}
),

source_coverage as (
    select
        kanoniv_id,
        count(distinct source_system)                           as source_count,
        listagg(distinct source_system, ', ') within group (order by source_system)
                                                                as source_systems,
        count(*)                                                as total_source_records
    from crosswalk
    group by kanoniv_id
)

select
    c.kanoniv_id,
    c.email,
    c.phone,
    c.first_name,
    c.last_name,
    c.company_name,
    c.confidence_score,
    sc.source_count,
    sc.source_systems,
    sc.total_source_records
from customers c
left join source_coverage sc on c.kanoniv_id = sc.kanoniv_id
