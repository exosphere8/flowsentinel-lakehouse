-- History of the hostname each IP address served, as a type 2 slowly changing dimension.
--
-- Built from event time, not from when the pipeline ran, so it is deterministic and correct
-- when data arrives late or is replayed. Per address and hour the dominant name wins (servers
-- behind CDNs or virtual hosting can present several); consecutive hours with the same name
-- form one version (the gaps-and-islands pattern).

with named as (
    select responder_ip as ip, flow_hour, responder_name as hostname, count(*) as flows
    from {{ ref('fct_flows') }}
    where responder_name is not null
    group by all
),

hourly as (
    select ip, flow_hour, hostname, flows
    from named
    qualify row_number() over (partition by ip, flow_hour order by flows desc, hostname) = 1
),

changes as (
    select
        *,
        case
            when hostname is distinct from lag(hostname) over (partition by ip order by flow_hour)
                then 1
            else 0
        end as is_change
    from hourly
),

islands as (
    select
        *,
        sum(is_change) over (partition by ip order by flow_hour rows unbounded preceding) as island
    from changes
),

versions as (
    select
        ip,
        hostname,
        min(flow_hour) as valid_from,
        max(flow_hour) + interval 1 hour as observed_until,
        sum(flows) as flows
    from islands
    group by ip, island, hostname
)

select
    md5(ip || '|' || cast(valid_from as varchar)) as host_name_key,
    ip,
    hostname,
    row_number() over (partition by ip order by valid_from) as version,
    valid_from,
    lead(valid_from) over (partition by ip order by valid_from) as valid_to,
    lead(valid_from) over (partition by ip order by valid_from) is null as is_current,
    observed_until,
    flows
from versions
