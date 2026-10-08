-- Bronze flow records with derived time and service columns. A view: filters on ingest_date
-- and ingested_at are pushed down to the Parquet scan, so incremental models read only new
-- partitions.
with flows as (
    select * from {{ source('bronze', 'flows') }}
)

select
    record_id,
    batch_id,
    source,
    sensor_id,
    capture_id,
    capture_file,
    capture_completion_state,
    contract_version,
    ingest_date,
    ingested_at,
    flow_id,
    ip_version,
    protocol,
    coalesce(protocol_name, 'IP-' || protocol) as protocol_name,
    initiator_ip,
    initiator_port,
    responder_ip,
    responder_port,
    initiator_basis,
    first_seen,
    last_seen,
    date_trunc('hour', first_seen) as flow_hour,
    cast(first_seen as date) as event_date,
    duration_seconds,
    i2r_packets,
    i2r_bytes,
    i2r_payload_bytes,
    r2i_packets,
    r2i_bytes,
    r2i_payload_bytes,
    packets_total,
    bytes_total,
    pkt_size_mean,
    pkt_size_stddev,
    iat_mean_seconds,
    iat_stddev_seconds,
    tcp_state,
    tcp_flags_initiator,
    tcp_flags_responder,
    tcp_rst_packets,
    app_protocols,
    coalesce(app_protocols[1], 'none') as app_protocol,
    dns_queries,
    responder_dns_names,
    http_hosts,
    http_paths,
    tls_server_names,
    -- The best name for the responder: TLS SNI, then HTTP Host (without a port), then a name
    -- that an earlier DNS answer in the same capture mapped to the responder.
    coalesce(
        tls_server_names[1],
        regexp_replace(http_hosts[1], '^(\[[^\]]*\]|[^:]*):[0-9]+$', '\1'),
        responder_dns_names[1]
    ) as responder_name,
    dominant_endpoint,
    end_reason,
    flow_warnings,
    len(flow_warnings) > 0 as has_warnings
from flows
