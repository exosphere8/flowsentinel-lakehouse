-- Reconciliation: the hourly aggregate must account for every flow and byte in fct_flows.
-- Returns a row (and fails) when the totals differ.
with fact as (
    select count(*) as flows, coalesce(sum(bytes_total), 0) as bytes from {{ ref('fct_flows') }}
),

aggregate as (
    select coalesce(sum(flows), 0) as flows, coalesce(sum(bytes), 0) as bytes
    from {{ ref('agg_traffic_hourly') }}
)

select fact.flows as fact_flows, aggregate.flows as aggregate_flows,
    fact.bytes as fact_bytes, aggregate.bytes as aggregate_bytes
from fact, aggregate
where fact.flows != aggregate.flows or fact.bytes != aggregate.bytes
