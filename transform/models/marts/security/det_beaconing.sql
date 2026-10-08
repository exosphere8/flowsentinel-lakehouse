-- Command-and-control beaconing (MITRE ATT&CK T1071): an internal host connects to the same
-- external endpoint many times a day at very regular intervals. Regularity is measured by the
-- coefficient of variation (stddev / mean) of the gaps between connection starts: about 1 for
-- human-driven traffic, close to 0 for a timer.
with outbound as (
    select initiator_ip, responder_ip, responder_port, event_date, first_seen, responder_name
    from {{ ref('fct_flows') }}
    where direction = 'outbound' and protocol in (6, 17)
),

gaps as (
    select
        *,
        epoch(first_seen) - lag(epoch(first_seen)) over (
            partition by initiator_ip, responder_ip, responder_port, event_date
            order by first_seen
        ) as gap_seconds
    from outbound
),

pairs as (
    select
        initiator_ip as src_ip,
        responder_ip as dst_ip,
        responder_port as dst_port,
        event_date,
        count(*) as connections,
        avg(gap_seconds) as mean_interval_seconds,
        stddev_samp(gap_seconds) as stddev_interval_seconds,
        min(first_seen) as first_seen,
        max(first_seen) as last_seen,
        mode(responder_name) as dst_name
    from gaps
    group by all
),

allowlist as (
    select * from {{ ref('detection_allowlist') }} where rule_id = 'beaconing'
)

select
    p.*,
    p.stddev_interval_seconds / p.mean_interval_seconds as interval_cv
from pairs as p
where p.connections >= {{ var('beaconing_min_connections') }}
    and p.mean_interval_seconds
        between {{ var('beaconing_min_interval_seconds') }} and {{ var('beaconing_max_interval_seconds') }}
    and p.stddev_interval_seconds / p.mean_interval_seconds <= {{ var('beaconing_max_cv') }}
    and not exists (
        select 1
        from allowlist as a
        where (a.src_ip = '*' or a.src_ip = p.src_ip)
            and (a.dst_ip = '*' or a.dst_ip = p.dst_ip)
            and (a.dst_port = '*' or a.dst_port = cast(p.dst_port as varchar))
    )
