-- One row per IP address seen in any flow, with its activity profile.
with endpoints as (
    select
        initiator_ip as ip,
        initiator_zone as zone,
        initiator_is_internal as is_internal,
        first_seen,
        last_seen,
        i2r_bytes as bytes_sent,
        r2i_bytes as bytes_received,
        1 as initiated,
        responder_ip as peer_ip,
        cast(null as varchar) as observed_name
    from {{ ref('fct_flows') }}

    union all

    select
        responder_ip as ip,
        responder_zone as zone,
        responder_is_internal as is_internal,
        first_seen,
        last_seen,
        r2i_bytes as bytes_sent,
        i2r_bytes as bytes_received,
        0 as initiated,
        initiator_ip as peer_ip,
        responder_name as observed_name
    from {{ ref('fct_flows') }}
)

select
    ip,
    -- The latest classification wins if the zone seed changed over time.
    arg_max(zone, last_seen) as zone,
    arg_max(is_internal, last_seen) as is_internal,
    min(first_seen) as first_seen,
    max(last_seen) as last_seen,
    count(*) as flows,
    sum(initiated) as flows_initiated,
    count(*) - sum(initiated) as flows_received,
    sum(bytes_sent) as bytes_sent,
    sum(bytes_received) as bytes_received,
    count(distinct peer_ip) as distinct_peers,
    mode(observed_name) as most_common_name
from endpoints
group by ip
