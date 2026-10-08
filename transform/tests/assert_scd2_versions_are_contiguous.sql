-- SCD type 2 invariants: per IP address, versions do not overlap, each version ends where the
-- next one starts, and exactly one version is current.
with versions as (
    select
        ip,
        version,
        valid_from,
        valid_to,
        is_current,
        lead(valid_from) over (partition by ip order by version) as next_valid_from
    from {{ ref('dim_host_names_scd2') }}
)

select ip, version, 'gap or overlap with the next version' as problem
from versions
where next_valid_from is not null and valid_to != next_valid_from

union all

select ip, null, 'not exactly one current version'
from versions
group by ip
having count(*) filter (where is_current) != 1
