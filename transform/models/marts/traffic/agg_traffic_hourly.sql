{{
    config(
        materialized='incremental',
        unique_key='hour_start',
        incremental_strategy='delete+insert',
    )
}}

-- Traffic per hour, direction, protocol and service.
--
-- Late data changes old hours, so incremental runs find every hour that received new or
-- replaced flows (by fct_flows.loaded_at) and recompute those hours completely. delete+insert
-- on hour_start replaces all rows of a recomputed hour. A full refresh gives the same result;
-- tests/test_pipeline.py::test_incremental_runs_with_late_data_match_a_full_refresh checks it.

with
{% if is_incremental() %}
changed_hours as (
    select distinct flow_hour
    from {{ ref('fct_flows') }}
    where loaded_at > (
        select coalesce(max(max_loaded_at), timestamptz '1970-01-01 00:00:00+00') from {{ this }}
    )
),
{% endif %}

flows as (
    select *
    from {{ ref('fct_flows') }}
    {% if is_incremental() %}
    where flow_hour in (select flow_hour from changed_hours)
    {% endif %}
)

select
    flow_hour as hour_start,
    direction,
    protocol_name,
    service,
    count(*) as flows,
    sum(packets_total) as packets,
    sum(bytes_total) as bytes,
    sum(i2r_bytes) as bytes_from_initiators,
    sum(r2i_bytes) as bytes_from_responders,
    count(distinct initiator_ip) as distinct_initiators,
    count(distinct responder_ip) as distinct_responders,
    max(loaded_at) as max_loaded_at
from flows
group by all
