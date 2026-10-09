"""Deployment configuration: validation, the prepared dbt project and its effect on results."""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from flowlake.bronze import Lake
from flowlake.evaluate import connect
from flowlake.ingest import ingest_path
from flowlake.project import ConfigError, load_config, packaged_project_dir, prepare_project
from flowlake.transform import build

ZONES = "zone,cidr,is_internal,description\n"
ALLOW = "rule_id,src_ip,dst_ip,dst_port,reason\n"


def config(tmp_path: Path, **files: str) -> Path:
    directory = tmp_path / "config"
    directory.mkdir(exist_ok=True)
    for name, content in files.items():
        (directory / name.replace("__", ".")).write_text(content)
    return directory


def test_defaults_prepare_a_reusable_project(lake: Lake) -> None:
    first = prepare_project(lake)
    assert (first.project_dir / "dbt_project.yml").exists()
    assert not (first.project_dir / "target").exists()
    assert first.variables == {}
    assert prepare_project(lake).project_dir == first.project_dir


def test_each_configuration_gets_its_own_project(lake: Lake, tmp_path: Path) -> None:
    directory = config(
        tmp_path,
        network_zones__csv=ZONES + "office,10.20.0.0/16,true,Office\n",
        detections__yml="beaconing_max_cv: 0.1\nport_scan_min_distinct_ports: 30\n",
    )
    settings = prepare_project(lake, directory)
    assert settings.project_dir != prepare_project(lake).project_dir
    zones = (settings.project_dir / "seeds" / "network_zones.csv").read_text()
    assert zones == ZONES + "office,10.20.0.0/16,true,Office\n"
    assert settings.variables == {"beaconing_max_cv": 0.1, "port_scan_min_distinct_ports": 30}


def _prepare(root: str) -> str:
    return str(prepare_project(Lake(Path(root))).project_dir)


def test_concurrent_processes_agree_on_one_project(tmp_path: Path) -> None:
    with ProcessPoolExecutor(max_workers=4) as pool:
        results = set(pool.map(_prepare, [str(tmp_path / "lake")] * 8))
    assert len(results) == 1
    projects = tmp_path / "lake" / ".dbt" / "projects"
    assert [p.name for p in projects.iterdir()] == [Path(results.pop()).name]


@pytest.mark.parametrize(
    ("files", "message"),
    [
        ({"network_zones__csv": "zone,cidr\nx,10.0.0.0/8\n"}, "first line must be"),
        ({"network_zones__csv": ZONES + "x,10.0.0.1/8,true,d\n"}, "line 2: cidr '10.0.0.1/8'"),
        ({"network_zones__csv": ZONES + "x,fd00::/8,true,d\n"}, "only IPv4"),
        ({"network_zones__csv": ZONES + "x,10.0.0.0/8,yes,d\n"}, "true or false"),
        ({"network_zones__csv": ZONES}, "no rows"),
        ({"detection_allowlist__csv": ALLOW + "beacon,*,*,123,r\n"}, "unknown rule_id"),
        ({"detection_allowlist__csv": ALLOW + "beaconing,host-1,*,123,r\n"}, "src_ip 'host-1'"),
        ({"detection_allowlist__csv": ALLOW + "beaconing,*,*,70000,r\n"}, "dst_port '70000'"),
        ({"detections__yml": "beaconing_max_cvv: 0.1\n"}, "unknown setting 'beaconing_max_cvv'"),
        ({"detections__yml": "port_scan_min_distinct_ports: 2.5\n"}, "must be a whole number"),
        ({"detections__yml": "beaconing_max_cv: fast\n"}, "must be a number"),
        ({"detections__yml": "beaconing_max_cv: -1\n"}, "must not be negative"),
        ({"detections__yml": "- a list\n"}, "name: value pairs"),
        ({"detections__yml": "a: [\n"}, "not valid YAML"),
    ],
)
def test_invalid_configuration_fails_with_a_clear_message(
    tmp_path: Path, files: dict[str, str], message: str
) -> None:
    with pytest.raises(ConfigError, match=message.replace("(", r"\(").replace("*", r"\*")):
        load_config(config(tmp_path, **files), packaged_project_dir())


def test_a_missing_configuration_directory_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope", packaged_project_dir())


@pytest.mark.slow
def test_configuration_changes_zones_allowlist_and_thresholds(
    tmp_path: Path, landing: tuple[Path, dict[str, Any]]
) -> None:
    truth = landing[1]
    exfil = next(i for i in truth["incidents"] if i["rule_id"] == "exfiltration")
    scan = next(i for i in truth["incidents"] if i["rule_id"] == "port_scan")
    directory = config(
        tmp_path,
        network_zones__csv=ZONES + "office,10.20.0.0/16,true,Office\n"
        "private,10.0.0.0/8,true,Rest\n",
        detection_allowlist__csv=ALLOW
        + f"exfiltration,{exfil['src_ip']},{exfil['dst_ip']},*,Approved offsite backup\n",
        detections__yml="port_scan_min_distinct_ports: 1000\n",
    )
    lake = Lake(tmp_path / "lake")
    ingest_path(lake, landing[0], workers=2)
    assert build(lake, config_dir=directory).success
    with connect(lake) as connection:
        rules = {r[0] for r in connection.execute("select rule_id from gold.fct_alerts").fetchall()}
        zone = connection.execute(
            "select zone from gold.dim_hosts where ip = ?", [scan["src_ip"]]
        ).fetchone()
    assert rules == {"beaconing", "dns_tunneling"}  # exfiltration allowlisted, scan threshold
    assert zone == ("office",)
