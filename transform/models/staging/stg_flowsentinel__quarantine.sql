select
    batch_id,
    record_index,
    source,
    sensor_id,
    capture_id,
    ingest_date,
    ingested_at,
    error_type,
    error_message,
    raw_record
from {{ source('bronze', 'quarantine') }}
