-- Flows as OCSF Network Activity events (class 4001, activity 6 "Traffic"), the schema used by
-- security data lakes such as Amazon Security Lake. Times are milliseconds since the epoch.
-- Export with, for example:
--   COPY (SELECT * FROM export.ocsf_network_activity) TO 'ocsf.json' (FORMAT json);
select
    4001 as class_uid,
    'Network Activity' as class_name,
    4 as category_uid,
    'Network Activity' as category_name,
    6 as activity_id,
    'Traffic' as activity_name,
    400106 as type_uid,
    'Network Activity: Traffic' as type_name,
    1 as severity_id,
    'Informational' as severity,
    epoch_ms(last_seen) as "time",
    epoch_ms(first_seen) as start_time,
    epoch_ms(last_seen) as end_time,
    cast(round(duration_seconds * 1000) as bigint) as duration,
    {'ip': initiator_ip, 'port': initiator_port} as src_endpoint,
    {'ip': responder_ip, 'port': responder_port, 'hostname': responder_name} as dst_endpoint,
    {
        'protocol_num': protocol,
        'protocol_name': lower(protocol_name),
        'protocol_ver_id': ip_version,
        'direction_id': case direction
            when 'inbound' then 1
            when 'outbound' then 2
            when 'internal' then 3
            else 0
        end,
        'direction': case direction
            when 'inbound' then 'Inbound'
            when 'outbound' then 'Outbound'
            when 'internal' then 'Lateral'
            else 'Unknown'
        end
    } as connection_info,
    {
        'bytes': bytes_total,
        'packets': packets_total,
        'bytes_out': i2r_bytes,
        'packets_out': i2r_packets,
        'bytes_in': r2i_bytes,
        'packets_in': r2i_packets
    } as traffic,
    {
        'version': '1.3.0',
        'uid': record_id,
        'product': {'name': 'FlowSentinel', 'vendor_name': 'exosphere8'}
    } as metadata
from {{ ref('fct_flows') }}
