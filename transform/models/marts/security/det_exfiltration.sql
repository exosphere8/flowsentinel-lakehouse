-- Large outbound transfers (MITRE ATT&CK T1048): an internal host uploads an unusually large
-- volume to one external host in a day, and the traffic is mostly upload.
select
    initiator_ip as src_ip,
    responder_ip as dst_ip,
    event_date,
    count(*) as flows,
    sum(i2r_bytes) as bytes_out,
    sum(r2i_bytes) as bytes_in,
    sum(i2r_bytes) / sum(bytes_total) as upload_ratio,
    min(first_seen) as first_seen,
    max(last_seen) as last_seen,
    mode(responder_name) as dst_name
from {{ ref('fct_flows') }}
where direction = 'outbound'
group by initiator_ip, responder_ip, event_date
having sum(i2r_bytes) >= {{ var('exfiltration_min_bytes_out') }}
    and sum(i2r_bytes) / sum(bytes_total) >= {{ var('exfiltration_min_upload_ratio') }}
