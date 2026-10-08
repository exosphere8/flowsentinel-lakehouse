{#- IPv4 helpers in plain SQL, so no DuckDB extension has to be downloaded at run time.
    Addresses are already normalized by the ingestion contract. -#}

{% macro ipv4_to_int(column) -%}
    case when {{ column }} not like '%:%' then
        (cast(split_part({{ column }}, '.', 1) as bigint) << 24)
        + (cast(split_part({{ column }}, '.', 2) as bigint) << 16)
        + (cast(split_part({{ column }}, '.', 3) as bigint) << 8)
        + cast(split_part({{ column }}, '.', 4) as bigint)
    end
{%- endmacro %}

{#- Unique local (fc00::/7) and link-local (fe80::/10) IPv6 addresses. -#}
{% macro is_private_ipv6(column) -%}
    ({{ column }} like '%:%' and regexp_matches(lower({{ column }}), '^(f[cd]|fe[89ab])'))
{%- endmacro %}

{#- The last two labels of a DNS name ("a.b.tunnel.example" -> "tunnel.example"). This ignores
    multi-label public suffixes such as co.uk; see the README's known limitations. -#}
{% macro parent_domain(column) -%}
    case
        when len(string_split({{ column }}, '.')) >= 2
            then array_to_string(list_slice(string_split({{ column }}, '.'), -2, -1), '.')
        else {{ column }}
    end
{%- endmacro %}
