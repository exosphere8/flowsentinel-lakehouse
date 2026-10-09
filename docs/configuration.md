# Configuring the lakehouse for your network

The lakehouse works out of the box, but it only knows the private address ranges. Tell it
which ranges are yours, which traffic is expected, and how sensitive each detection should be.

All of it lives in one directory with up to three files. In the self-hosted suite that is
`deploy/config/`, or the directory that `CONFIG_DIR` in `.env` names. On the command line, pass
`--config DIR` or set `FLOWLAKE_CONFIG`. Every file is optional, and
[`deploy/config-examples/`](../deploy/config-examples) has an example of each:

```bash
cp deploy/config-examples/* deploy/config/   # then edit
```

| File | Replaces | Use it for |
| --- | --- | --- |
| `network_zones.csv` | the default zones | your subnets, so direction (inbound, outbound, internal) is right |
| `detection_allowlist.csv` | the default allowlist | known-good traffic that looks like an attack |
| `detections.yml` | single thresholds | making a detection more or less sensitive |

## Checking a configuration

```bash
flowlake --config deploy/config config                          # on the command line
docker compose exec lakehouse-daemon flowlake config            # in the suite
```

Every file is validated before anything runs. A typo stops the pipeline with the file, line and
reason, for example `network_zones.csv line 4: cidr '10.0.0.1/8': 10.0.0.1/8 has host bits
set`, instead of
quietly misclassifying traffic. The suite's containers check the configuration when they start,
so a bad file shows up in `docker compose logs`.

## Network zones

`network_zones.csv` replaces the built-in zones, so include everything you want classified:

```csv
zone,cidr,is_internal,description
servers,10.0.10.0/24,true,Data center servers
workstations,10.0.20.0/22,true,Office workstations
guest_wifi,192.168.50.0/24,false,Guest Wi-Fi: treat like the internet
public_web,203.0.113.0/28,true,Our own public web servers
private,10.0.0.0/8,true,Other RFC 1918 space
```

* `cidr` must be an IPv4 network written exactly (`10.0.0.0/8`, not `10.0.0.1/8`).
* `is_internal` is `true` or `false`. A flow between two internal addresses is `internal`; one
  started by an internal address is `outbound`, one answered by an internal address `inbound`,
  and anything else `external`.
* When ranges overlap, the most specific one wins, so a catch-all like `10.0.0.0/8` can follow
  the subnets carved out of it.
* Addresses in no range are external. Add the public ranges you own as internal, or traffic to
  your own web servers counts as outbound.

## Allowlist

Each row silences one kind of alert for matching traffic. `*` matches anything:

```csv
rule_id,src_ip,dst_ip,dst_port,reason
beaconing,*,*,123,NTP time synchronization is periodic by design.
beaconing,10.0.10.5,*,443,Monitoring agent reports in every 60 seconds.
exfiltration,10.0.10.20,198.51.100.25,*,Nightly offsite backup.
```

`rule_id` is one of `port_scan`, `beaconing`, `dns_tunneling`, `exfiltration`. Addresses are
single IP addresses, not ranges, and ports are 0 to 65535. Write a real `reason`: it is the only record of
why an alert is hidden. The file replaces the default, so keep the NTP row if you still want it.

## Thresholds

`detections.yml` overrides only the settings it names:

```yaml
port_scan_min_distinct_ports: 50
exfiltration_min_bytes_out: 1000000000   # 1 GB a day to one host
```

| Setting | Default | Meaning |
| --- | ---: | --- |
| `port_scan_min_distinct_ports` | 50 | distinct ports one source probes on one host within an hour |
| `port_scan_min_failed_ratio` | 0.8 | share of those connections that failed |
| `beaconing_min_connections` | 20 | connections a day from one host to one endpoint |
| `beaconing_max_cv` | 0.2 | how regular the gaps must be (standard deviation / mean) |
| `beaconing_min_interval_seconds` | 10 | shortest mean gap that counts |
| `beaconing_max_interval_seconds` | 3600 | longest mean gap that counts |
| `exfiltration_min_bytes_out` | 500000000 | bytes uploaded to one external host in a day |
| `exfiltration_min_upload_ratio` | 0.8 | share of the traffic with that host that is upload |
| `dns_tunneling_min_distinct_names` | 50 | distinct names under one parent domain within an hour |
| `dns_tunneling_min_avg_label_length` | 20 | average length of the leftmost label |
| `incremental_lookback_minutes` | 30 | how far back each run re-reads already loaded data |

Unknown names, wrong types and negative values are errors. The dbt unit tests keep their own
fixed thresholds, so tuning cannot make them fail.

## When changes take effect

In the suite, restart the pipeline containers after editing the directory:

```bash
docker compose restart lakehouse-ui lakehouse-daemon
```

Thresholds and the allowlist apply from the next run, to all data, because the detection tables
are rebuilt on every run. Zones are different: flows already in `fct_flows` keep the direction
they were given when they were loaded. To reclassify history after changing zones, run one full
refresh (see [deployment.md](deployment.md#rebuilding-everything)).
