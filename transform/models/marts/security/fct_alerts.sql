-- Every detection in one shape, enriched with rule metadata and MITRE ATT&CK names.
-- alert_id is a hash of the rule and the detection's key, so it is stable across rebuilds.
with alerts as (
    select
        'port_scan' as rule_id,
        src_ip,
        dst_ip,
        cast(null as integer) as dst_port,
        cast(null as varchar) as dst_name,
        window_start,
        window_end,
        flows,
        bytes,
        to_json({
            'distinct_ports': distinct_ports,
            'failed_ratio': round(failed_ratio, 3),
            'sample_ports': sample_ports
        }) as evidence
    from {{ ref('det_port_scan') }}

    union all

    select
        'beaconing',
        src_ip,
        dst_ip,
        dst_port,
        dst_name,
        cast(event_date as timestamptz),
        cast(event_date as timestamptz) + interval 1 day,
        connections,
        cast(null as bigint),
        to_json({
            'connections': connections,
            'mean_interval_seconds': round(mean_interval_seconds, 1),
            'interval_cv': round(interval_cv, 4),
            'first_seen': first_seen,
            'last_seen': last_seen
        })
    from {{ ref('det_beaconing') }}

    union all

    select
        'exfiltration',
        src_ip,
        dst_ip,
        cast(null as integer),
        dst_name,
        cast(event_date as timestamptz),
        cast(event_date as timestamptz) + interval 1 day,
        flows,
        bytes_out,
        to_json({
            'bytes_out': bytes_out,
            'bytes_in': bytes_in,
            'upload_ratio': round(upload_ratio, 4),
            'first_seen': first_seen,
            'last_seen': last_seen
        })
    from {{ ref('det_exfiltration') }}

    union all

    select
        'dns_tunneling',
        src_ip,
        cast(null as varchar),
        53,
        parent_domain,
        window_start,
        window_end,
        queries,
        cast(null as bigint),
        to_json({
            'distinct_names': distinct_names,
            'avg_label_length': round(avg_label_length, 1),
            'sample_query': sample_query
        })
    from {{ ref('det_dns_tunneling') }}
)

select
    md5(concat_ws('|', a.rule_id, a.src_ip, a.dst_ip, a.dst_name, cast(a.window_start as varchar)))
        as alert_id,
    a.rule_id,
    r.rule_name,
    r.severity,
    r.mitre_technique_id,
    m.technique_name as mitre_technique_name,
    m.tactic as mitre_tactic,
    a.src_ip,
    a.dst_ip,
    a.dst_port,
    a.dst_name,
    a.window_start,
    a.window_end,
    a.flows,
    a.bytes,
    a.evidence
from alerts as a
inner join {{ ref('detection_rules') }} as r on r.rule_id = a.rule_id
inner join {{ ref('mitre_techniques') }} as m on m.technique_id = r.mitre_technique_id
