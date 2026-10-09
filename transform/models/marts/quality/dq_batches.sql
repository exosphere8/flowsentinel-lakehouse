-- Data quality per ingestion batch: how many records were accepted and quarantined, and why.
with accepted as (
    select batch_id, any_value(sensor_id) as sensor_id, any_value(source) as source,
        any_value(capture_file) as capture_file,
        any_value(capture_completion_state) as capture_completion_state,
        max(ingested_at) as ingested_at,
        count(*) as records_accepted, count(distinct record_id) as distinct_records
    from {{ ref('stg_flowsentinel__flows') }}
    group by batch_id
),

rejected as (
    select batch_id, any_value(sensor_id) as sensor_id, any_value(source) as source,
        max(ingested_at) as ingested_at, count(*) as records_quarantined,
        histogram(error_type) as quarantine_reasons
    from {{ ref('stg_flowsentinel__quarantine') }}
    group by batch_id
)

select
    coalesce(a.batch_id, r.batch_id) as batch_id,
    coalesce(a.sensor_id, r.sensor_id) as sensor_id,
    coalesce(a.source, r.source) as source,
    a.capture_file,
    -- "complete", or why FlowSentinel stopped early (packet_limit_reached, time_limit_reached).
    a.capture_completion_state,
    coalesce(a.capture_completion_state, 'complete') != 'complete' as capture_was_cut_short,
    coalesce(a.ingested_at, r.ingested_at) as ingested_at,
    coalesce(a.records_accepted, 0) as records_accepted,
    coalesce(r.records_quarantined, 0) as records_quarantined,
    coalesce(r.records_quarantined, 0)
        / (coalesce(a.records_accepted, 0) + coalesce(r.records_quarantined, 0)) as quarantine_rate,
    r.quarantine_reasons
from accepted as a
full outer join rejected as r on r.batch_id = a.batch_id
