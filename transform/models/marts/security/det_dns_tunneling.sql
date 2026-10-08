-- DNS tunneling (MITRE ATT&CK T1071.004): one host asks many distinct, long names under the
-- same parent domain within an hour; data is encoded in the leftmost label.
with queries as (
    select f.initiator_ip, f.flow_hour, f.first_seen, lower(q.query) as query
    from {{ ref('fct_flows') }} as f, unnest(f.dns_queries) as q(query)
    where f.app_protocol = 'dns' or f.service = 'dns'
),

parsed as (
    select
        *,
        {{ parent_domain('query') }} as parent_domain,
        split_part(query, '.', 1) as first_label
    from queries
)

select
    initiator_ip as src_ip,
    parent_domain,
    flow_hour as window_start,
    flow_hour + interval 1 hour as window_end,
    count(*) as queries,
    count(distinct query) as distinct_names,
    avg(length(first_label)) as avg_label_length,
    max(length(query)) as max_name_length,
    min(first_seen) as first_seen,
    max(first_seen) as last_seen,
    min(query) as sample_query
from parsed
group by initiator_ip, parent_domain, flow_hour
having count(distinct query) >= {{ var('dns_tunneling_min_distinct_names') }}
    and avg(length(first_label)) >= {{ var('dns_tunneling_min_avg_label_length') }}
