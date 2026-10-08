-- Port scans (MITRE ATT&CK T1046): one host tries many ports on another host within an hour
-- and most attempts fail (reset or never answered).
select
    initiator_ip as src_ip,
    responder_ip as dst_ip,
    flow_hour as window_start,
    flow_hour + interval 1 hour as window_end,
    count(*) as flows,
    count(distinct responder_port) as distinct_ports,
    avg(case when tcp_state in ('reset', 'syn_sent') then 1.0 else 0.0 end) as failed_ratio,
    sum(bytes_total) as bytes,
    min(first_seen) as first_seen,
    max(last_seen) as last_seen,
    list_sort(list(distinct responder_port))[1:10] as sample_ports
from {{ ref('fct_flows') }}
where protocol = 6
group by initiator_ip, responder_ip, flow_hour
having count(distinct responder_port) >= {{ var('port_scan_min_distinct_ports') }}
    and avg(case when tcp_state in ('reset', 'syn_sent') then 1.0 else 0.0 end)
        >= {{ var('port_scan_min_failed_ratio') }}
