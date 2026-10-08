{{
    config(
        materialized='incremental',
        unique_key='record_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns',
    )
}}

-- One row per flow, deduplicated on record_id and classified by network zone.
--
-- Incremental runs read only bronze rows ingested since the last run, minus a lookback window
-- (see incremental_lookback_minutes). Bronze is partitioned by ingestion date, so late data for
-- an old event date still arrives in a new partition and is picked up here. Rows read twice are
-- replaced, not duplicated (delete+insert on record_id), which also makes a replayed batch or
-- the same capture ingested twice harmless.

with new_rows as (
    select *
    from {{ ref('stg_flowsentinel__flows') }}
    {% if is_incremental() %}
    -- coalesce: an empty target (for example after a run on an empty lake) means "read all".
    where ingested_at > (
            select coalesce(
                max(ingested_at) - interval {{ var('incremental_lookback_minutes') }} minute,
                timestamptz '1970-01-01 00:00:00+00'
            )
            from {{ this }}
        )
        -- Lets DuckDB skip older ingest_date partitions without opening their files.
        and ingest_date >= (
            select coalesce(
                cast(max(ingested_at) - interval {{ var('incremental_lookback_minutes') }} minute as date),
                date '1970-01-01'
            )
            from {{ this }}
        )
    {% endif %}
    qualify row_number() over (partition by record_id order by ingested_at desc, batch_id desc) = 1
),

addresses as (
    select initiator_ip as ip from new_rows
    union
    select responder_ip as ip from new_rows
),

-- The most specific zone wins; addresses in no zone are external.
address_zones as (
    select
        a.ip,
        coalesce(z.zone, case when {{ is_private_ipv6('a.ip') }} then 'private' else 'external' end) as zone,
        coalesce(z.is_internal, {{ is_private_ipv6('a.ip') }}) as is_internal
    from addresses as a
    left join {{ ref('stg_network_zones') }} as z
        on {{ ipv4_to_int('a.ip') }} between z.range_start and z.range_end
    qualify row_number() over (partition by a.ip order by z.prefix_length desc nulls last) = 1
),

services as (
    select * from {{ ref('service_ports') }}
)

select
    f.record_id,
    f.sensor_id,
    f.capture_id,
    f.capture_file,
    f.capture_completion_state,
    f.flow_id,
    f.event_date,
    f.flow_hour,
    f.first_seen,
    f.last_seen,
    f.duration_seconds,
    f.ip_version,
    f.protocol,
    f.protocol_name,
    f.initiator_ip,
    f.initiator_port,
    initiator_zone.zone as initiator_zone,
    initiator_zone.is_internal as initiator_is_internal,
    f.responder_ip,
    f.responder_port,
    responder_zone.zone as responder_zone,
    responder_zone.is_internal as responder_is_internal,
    case
        when initiator_zone.is_internal and responder_zone.is_internal then 'internal'
        when initiator_zone.is_internal then 'outbound'
        when responder_zone.is_internal then 'inbound'
        else 'external'
    end as direction,
    -- Name the service by the server side: the responder's port, else the initiator's.
    coalesce(responder_service.service, initiator_service.service, 'other') as service,
    f.app_protocol,
    f.responder_name,
    f.i2r_packets,
    f.i2r_bytes,
    f.r2i_packets,
    f.r2i_bytes,
    f.packets_total,
    f.bytes_total,
    f.i2r_payload_bytes + f.r2i_payload_bytes as payload_bytes,
    f.pkt_size_mean,
    f.iat_mean_seconds,
    f.iat_stddev_seconds,
    f.tcp_state,
    f.tcp_flags_initiator,
    f.tcp_flags_responder,
    f.dominant_endpoint,
    f.end_reason,
    f.dns_queries,
    f.http_hosts,
    f.http_paths,
    f.tls_server_names,
    f.app_protocols,
    f.has_warnings,
    f.flow_warnings,
    f.batch_id,
    f.source,
    f.ingested_at,
    -- When this row was (re)written to the warehouse; downstream incremental models use it to
    -- find the hours that changed.
    cast('{{ run_started_at.isoformat() }}' as timestamptz) as loaded_at
from new_rows as f
inner join address_zones as initiator_zone on initiator_zone.ip = f.initiator_ip
inner join address_zones as responder_zone on responder_zone.ip = f.responder_ip
left join services as responder_service
    on responder_service.protocol = f.protocol and responder_service.port = f.responder_port
left join services as initiator_service
    on initiator_service.protocol = f.protocol and initiator_service.port = f.initiator_port
