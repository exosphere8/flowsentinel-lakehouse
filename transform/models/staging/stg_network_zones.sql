-- Network zones as integer ranges, so flows can be classified with a range join.
with zones as (
    select * from {{ ref('network_zones') }}
),

parsed as (
    select
        zone,
        cidr,
        is_internal,
        split_part(cidr, '/', 1) as network,
        cast(split_part(cidr, '/', 2) as integer) as prefix_length
    from zones
)

select
    zone,
    cidr,
    is_internal,
    prefix_length,
    {{ ipv4_to_int('network') }} as range_start,
    {{ ipv4_to_int('network') }} + (cast(1 as bigint) << (32 - prefix_length)) - 1 as range_end
from parsed
