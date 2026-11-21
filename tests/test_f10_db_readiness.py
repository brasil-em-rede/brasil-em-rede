"""Opt-in production-profile API proof against an owned disposable Neo4j."""
from __future__ import annotations

import csv
import hashlib
import importlib.util
import io
import json
import os
import secrets
import socket
import tarfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import quote

import docker  # type: ignore[import-untyped]
import pytest

ROOT = Path(__file__).resolve().parents[1]
API_IMAGE = "bracc-f10-profile_api:f10-check"
API_IMAGE_ID = "sha256:4c9a285a4887f062488d6ae1ade0197e12d75506cb36b8742efda2f690ff6317"
NEO4J_IMAGE = "neo4j:5.26.21-community"
NEO4J_IMAGE_ID = "sha256:5cce41f4e2a2f69ac65b91769fd4e985668a8fe86e0e8c187830cc4de37722e9"
DB_READINESS_SKIP = pytest.mark.skipif(
    os.getenv("F10_RUN_DB_READINESS") != "1",
    reason="requires explicit Docker integration opt-in",
)


def _expected_public_source_ids() -> set[str]:
    registry = ROOT / "docs/source_registry_br_v1.csv"
    with registry.open(encoding="utf-8", newline="") as csv_file:
        rows = list(csv.DictReader(csv_file))
    public_ids = [
        row["source_id"].strip()
        for row in rows
        if row.get("in_universe_v1", "").strip().lower() == "true"
    ]
    assert all(public_ids), "Canonical public registry has an empty source ID"
    assert len(public_ids) == len(set(public_ids)), "Canonical public registry repeats source IDs"
    return set(public_ids)


def _assert_public_catalog_ids(actual_ids: list[str], expected_ids: set[str]) -> str:
    assert all(actual_ids), "HTTP catalog contains an empty source ID"
    assert len(actual_ids) == len(set(actual_ids)), "HTTP catalog repeats source IDs"
    assert set(actual_ids) == expected_ids, "HTTP catalog source IDs differ from canonical public CSV"
    canonical_ids = "\n".join(sorted(actual_ids)).encode()
    return hashlib.sha256(canonical_ids).hexdigest()


def _assert_fixture_count(output: str, expected: int) -> int:
    rows = list(csv.reader(io.StringIO(output.strip())))
    assert len(rows) == 2 and rows[0] == ["count"], "Unexpected cypher-shell scalar table shape"
    assert len(rows[1]) == 1, "Expected exactly one scalar count value"
    assert rows[1][0].isdigit(), "Cypher count result is not an integer"
    count = int(rows[1][0])
    assert count == expected, f"Expected exactly {expected} fixture nodes, got {count}"
    return count


def _validate_cnpj_with_etl_helper(cnpj: str) -> bool:
    validator_path = ROOT / "etl/src/bracc_etl/transforms/document_formatting.py"
    spec = importlib.util.spec_from_file_location("f10_document_formatting", validator_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return bool(module.validate_cnpj(cnpj))


def test_fixture_and_missing_cnpjs_validate_with_etl_helper() -> None:
    assert _validate_cnpj_with_etl_helper("11222333000181")
    assert _validate_cnpj_with_etl_helper("88888888000191")


def test_fixture_count_oracle_rejects_multidigit_count() -> None:
    assert _assert_fixture_count("count\n1\n", expected=1) == 1
    with pytest.raises(AssertionError, match="got 10"):
        _assert_fixture_count("count\n10\n", expected=1)


@pytest.mark.parametrize(
    "actual_ids",
    [
        ["cnpj", "tse", "unexpected"],
        ["cnpj"],
        ["cnpj", "tse", "tse"],
    ],
    ids=["extra", "missing", "duplicate"],
)
def test_public_catalog_oracle_rejects_mutated_source_ids(actual_ids: list[str]) -> None:
    with pytest.raises(AssertionError):
        _assert_public_catalog_ids(actual_ids, {"cnpj", "tse"})


def _unused_loopback_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _request(url: str, timeout: float = 3) -> tuple[int, Any]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        try:
            return error.code, json.load(error)
        except (ValueError, json.JSONDecodeError):
            return error.code, None


def _entity_url(base: str, identifier: str) -> str:
    return base + "/api/v1/entity/" + quote(identifier, safe="")


def _stage_file(container: Any, host_file: Path, target_dir: str) -> None:
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as tar:
        tar.add(host_file, arcname=host_file.name)
    archive.seek(0)
    if not container.put_archive(target_dir, archive):
        pytest.fail("Could not stage shipped probe in owned API container")


def _cypher_ready(container: Any, password: str, deadline: float) -> tuple[int | None, str]:
    """Poll Neo4j from inside its container; bridge IP is not host-routable here."""
    last_exit: int | None = None
    while time.monotonic() < deadline:
        container.reload()
        if container.status != "running":
            return last_exit, f"Neo4j container state={container.status}"
        result = container.exec_run(
            ["cypher-shell", "-u", "neo4j", "-p", password, "RETURN 1 AS ready"],
            demux=True,
        )
        last_exit = result.exit_code
        if last_exit == 0:
            return last_exit, "ready"
        time.sleep(1)
    return last_exit, "Neo4j did not become ready before deadline"


def _cypher(container: Any, password: str, statement: str) -> tuple[int, str]:
    result = container.exec_run(
        ["cypher-shell", "-u", "neo4j", "-p", password, statement], demux=True
    )
    stdout, _stderr = result.output or (b"", b"")
    return result.exit_code, (stdout or b"").decode(errors="replace")


@DB_READINESS_SKIP
def test_production_profile_readiness_entity_and_restart(tmp_path: Path) -> None:
    """Exercise real HTTP, shipped probe, DB outage/recovery and retained fixture."""
    suffix = secrets.token_hex(5)
    prefix = f"bracc-m0-20261121-f10-catalog-negative-{suffix}"
    network_name = f"{prefix}-net"
    db_name = f"{prefix}-neo4j"
    api_name = f"{prefix}-api"
    port = _unused_loopback_port()
    password = secrets.token_urlsafe(24)
    jwt_secret = secrets.token_urlsafe(40)
    fixture_cnpj = "11222333000181"
    missing_cnpj = "88888888000191"
    assert _validate_cnpj_with_etl_helper(fixture_cnpj)
    assert _validate_cnpj_with_etl_helper(missing_cnpj)
    expected_source_ids = _expected_public_source_ids()
    registry = ROOT / "docs/source_registry_br_v1.csv"
    expected_source_ids_sha256 = hashlib.sha256(
        "\n".join(sorted(expected_source_ids)).encode()
    ).hexdigest()
    fixture_path = tmp_path / "source_registry_br_v1.csv"
    canonical_bytes = registry.read_bytes()
    fixture_path.write_bytes(canonical_bytes)
    fixture_sha256 = hashlib.sha256(canonical_bytes).hexdigest()
    client = docker.from_env()
    created_network: Any | None = None
    created_db: Any | None = None
    created_api: Any | None = None
    evidence: dict[str, Any] = {
        "resource_prefix": prefix,
        "host_binding": f"127.0.0.1:{port}->8000/tcp",
        "fixture": f"synthetic Company CNPJ {fixture_cnpj}",
        "declared_catalog_count": None,
        "responses": {},
        "restart": {},
        "cleanup": {},
        "image_ids": {},
    }
    names = {"network": network_name, "database": db_name, "api": api_name}
    try:
        for name in (network_name, db_name, api_name):
            try:
                client.containers.get(name)
            except docker.errors.NotFound:
                pass
            else:
                pytest.fail(f"Refusing preexisting container: {name}")
        try:
            client.networks.get(network_name)
        except docker.errors.NotFound:
            pass
        else:
            pytest.fail(f"Refusing preexisting network: {network_name}")

        api_image = client.images.get(API_IMAGE)
        neo4j_image = client.images.get(NEO4J_IMAGE)
        assert api_image.id == API_IMAGE_ID, "API image ID differs from recorded candidate"
        assert neo4j_image.id == NEO4J_IMAGE_ID, "Neo4j image ID differs from recorded candidate"
        evidence["image_ids"] = {"api": api_image.id, "neo4j": neo4j_image.id}

        created_network = client.networks.create(
            network_name,
            driver="bridge",
            labels={"bracc.owner": "F10-CATALOG-NEGATIVE", "bracc.run": prefix},
        )
        created_db = client.containers.create(
            NEO4J_IMAGE,
            name=db_name,
            network=network_name,
            environment={
                "NEO4J_AUTH": f"neo4j/{password}",
                "NEO4J_server_memory_heap_initial__size": "256m",
                "NEO4J_server_memory_heap_max__size": "512m",
                "NEO4J_server_memory_pagecache_size": "128m",
            },
            mem_limit="1g",
            nano_cpus=1_000_000_000,
            labels={"bracc.owner": "F10-CATALOG-NEGATIVE", "bracc.run": prefix},
        )
        created_db.start()
        code, readiness_detail = _cypher_ready(created_db, password, time.monotonic() + 120)
        evidence["neo4j_startup"] = {"exit_code": code, "result": readiness_detail}
        assert code == 0, f"Owned Neo4j readiness failed: {readiness_detail}; exit={code}"

        # Run production startup as shipped; do not bypass the schema initializer.
        current_module_path = ROOT / "api/src/bracc/services/neo4j_service.py"
        current_module_hash = hashlib.sha256(current_module_path.read_bytes()).hexdigest()
        registry = ROOT / "docs/source_registry_br_v1.csv"
        current_registry_hash = fixture_sha256
        created_api = client.containers.create(
            API_IMAGE_ID,
            name=api_name,
            network=network_name,
            ports={"8000/tcp": ("127.0.0.1", port)},
            environment={
                "APP_ENV": "production",
                "PUBLIC_MODE": "true",
                "PATTERNS_ENABLED": "true",
                "PUBLIC_ALLOW_PERSON": "false",
                "PUBLIC_ALLOW_ENTITY_LOOKUP": "true",
                "PUBLIC_ALLOW_INVESTIGATIONS": "false",
                "AUTH_COOKIE_SECURE": "true",
                "AUTH_COOKIE_SAMESITE": "lax",
                "NEO4J_URI": f"bolt://{db_name}:7687",
                "NEO4J_USER": "neo4j",
                "NEO4J_PASSWORD": password,
                "NEO4J_DATABASE": "neo4j",
                "JWT_SECRET_KEY": jwt_secret,
                "CORS_ORIGINS": "https://f10.invalid",
                "PRODUCT_TIER": "community",
                "BRACC_SOURCE_REGISTRY_PATH": "/app/docs/source_registry_br_v1.csv",
            },
            mem_limit="768m",
            nano_cpus=1_000_000_000,
            labels={"bracc.owner": "F10-CATALOG-NEGATIVE", "bracc.run": prefix},
            volumes={
                str(ROOT / "api/src/bracc"): {"bind": "/app/bracc", "mode": "ro"},
                str(fixture_path): {"bind": "/app/docs/source_registry_br_v1.csv", "mode": "ro"},
            },
        )
        created_api.start()
        base = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            created_api.reload()
            if created_api.status == "exited":
                (tmp_path / "api-container.log").write_text(created_api.logs().decode(errors="replace"))
                pytest.fail("Production-profile API exited during startup; inspect private container log")
            try:
                status, live_body = _request(base + "/health", timeout=1)
                if status == 200 and live_body == {"status": "ok"}:
                    break
            except (OSError, TimeoutError):
                pass
            time.sleep(1)
        else:
            pytest.fail("Exclusive production-profile API did not become live")

        source_check = created_api.exec_run(
            [
                "/app/.venv/bin/python",
                "-c",
                "import bracc.services.neo4j_service as m,hashlib; from pathlib import Path; print(m.__file__); print(hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()); print(hashlib.sha256(Path('/app/docs/source_registry_br_v1.csv').read_bytes()).hexdigest())",
            ],
            demux=True,
        )
        source_stdout, source_stderr = source_check.output or (b"", b"")
        source_text = (source_stdout or b"").decode(errors="replace")
        source_lines = source_text.splitlines()
        if source_check.exit_code != 0:
            error_text = (source_stderr or b"").decode(errors="replace")
            (tmp_path / "source-binding-error.txt").write_text(error_text)
            pytest.fail(f"Container source-binding check failed with exit {source_check.exit_code}")
        assert len(source_lines) == 3
        assert source_lines[0] == "/app/bracc/services/neo4j_service.py"
        assert source_lines[1] == current_module_hash
        assert source_lines[2] == current_registry_hash
        evidence["source_binding"] = {
            "module_path": source_lines[0],
            "module_sha256": source_lines[1],
            "registry_path": "/app/docs/source_registry_br_v1.csv",
            "registry_sha256": source_lines[2],
        }

        probe = ROOT / "infra/scripts/api-readiness.py"
        probe_hash = hashlib.sha256(probe.read_bytes()).hexdigest()
        _stage_file(created_api, probe, "/tmp")
        script_path = "/tmp/api-readiness.py"
        evidence["probe_sha256"] = probe_hash

        # Synthetic fixture is isolated to this exclusive disposable Neo4j database.
        code, _output = _cypher(
            created_db,
            password,
            f"CREATE (:Company {{cnpj: '{fixture_cnpj}', name: 'Synthetic F10 Company'}})",
        )
        assert code == 0, "Synthetic fixture creation failed; output withheld"
        code, count_output = _cypher(created_db, password, "MATCH (n) RETURN count(n) AS count")
        assert code == 0, "Synthetic fixture verification query failed; output withheld"
        fixture_count = _assert_fixture_count(count_output, expected=1)
        evidence["fixture_count"] = fixture_count

        healthy = {
            "/health": _request(base + "/health"),
            "/api/v1/meta/health": _request(base + "/api/v1/meta/health"),
            "/api/v1/meta/sources": _request(base + "/api/v1/meta/sources"),
            "/api/v1/entity/11222333000181": _request(
                _entity_url(base, "11222333000181")
            ),
            f"/api/v1/entity/{missing_cnpj}": _request(
                _entity_url(base, missing_cnpj)
            ),
        }
        sources_response = healthy["/api/v1/meta/sources"]
        assert sources_response[0] == 200
        actual_source_ids = [source["id"] for source in sources_response[1]["sources"]]
        actual_source_ids_sha256 = _assert_public_catalog_ids(actual_source_ids, expected_source_ids)
        catalog_count = len(actual_source_ids)
        assert catalog_count == len(expected_source_ids)
        evidence["declared_catalog_count"] = catalog_count
        evidence["catalog_oracle"] = {
            "expected_count": len(expected_source_ids),
            "expected_ids_sha256": expected_source_ids_sha256,
            "actual_count": catalog_count,
            "actual_ids_sha256": actual_source_ids_sha256,
        }
        evidence["responses"]["healthy"] = {
            "/health": healthy["/health"],
            "/api/v1/meta/health": healthy["/api/v1/meta/health"],
            "/api/v1/meta/sources": {
                "status": sources_response[0],
                "count": catalog_count,
                "ids_sha256": actual_source_ids_sha256,
            },
            "/api/v1/entity/11222333000181": healthy["/api/v1/entity/11222333000181"],
            "/api/v1/entity/88888888000191": healthy[f"/api/v1/entity/{missing_cnpj}"],
        }
        assert healthy["/health"] == (200, {"status": "ok"})
        assert healthy["/api/v1/meta/health"] == (200, {"neo4j": "connected"})
        assert healthy["/api/v1/entity/11222333000181"][0] == 200
        assert (
            healthy["/api/v1/entity/11222333000181"][1]["properties"]["name"]
            == "Synthetic F10 Company"
        )
        assert healthy[f"/api/v1/entity/{missing_cnpj}"][0] == 404
        assert catalog_count > 0
        probe_ok = created_api.exec_run(["/app/.venv/bin/python", script_path], demux=True)
        assert probe_ok.exit_code == 0, "Shipped readiness probe failed while DB was healthy"

        # Empty only the private CSV fixture; the API reads it per request.
        evidence["catalog_fixture"] = {
            "path": str(fixture_path),
            "canonical_sha256": fixture_sha256,
            "canonical_bytes": len(canonical_bytes),
        }
        fixture_path.write_bytes(b"")
        assert _request(base + "/api/v1/meta/sources") == (200, {"sources": []})
        assert _request(base + "/health") == (200, {"status": "ok"})
        assert _request(base + "/api/v1/meta/health") == (200, {"neo4j": "connected"})
        probe_empty = created_api.exec_run(["/app/.venv/bin/python", script_path], demux=True)
        evidence["catalog_fixture"]["empty_probe_exit"] = probe_empty.exit_code
        assert probe_empty.exit_code != 0, "Shipped readiness probe accepted an empty catalog"

        fixture_path.write_bytes(canonical_bytes)
        assert fixture_path.read_bytes() == canonical_bytes
        restored = _request(base + "/api/v1/meta/sources")
        assert restored[0] == 200
        restored_ids = [source["id"] for source in restored[1]["sources"]]
        assert (
            _assert_public_catalog_ids(restored_ids, expected_source_ids)
            == expected_source_ids_sha256
        )
        probe_restored = created_api.exec_run(["/app/.venv/bin/python", script_path], demux=True)
        assert probe_restored.exit_code == 0, (
            "Shipped readiness probe failed after registry restoration"
        )
        evidence["catalog_fixture"]["restored"] = {
            "count": len(restored_ids),
            "sha256": fixture_sha256,
        }

        created_db.stop(timeout=10)
        outage = {
            "/health": _request(base + "/health"),
            "/api/v1/meta/health": _request(base + "/api/v1/meta/health"),
        }
        evidence["responses"]["database_unavailable"] = outage
        assert outage["/health"] == (200, {"status": "ok"})
        assert outage["/api/v1/meta/health"][0] >= 500
        probe_down = created_api.exec_run(["/app/.venv/bin/python", script_path], demux=True)
        assert probe_down.exit_code != 0

        created_db.start()
        code, detail = _cypher_ready(created_db, password, time.monotonic() + 120)
        assert code == 0, f"Owned Neo4j restart readiness failed: {detail}; exit={code}"
        recovered_status = 0
        recovered_body: Any = None
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                recovered_status, recovered_body = _request(base + "/api/v1/meta/health", timeout=2)
                if recovered_status == 200 and recovered_body == {"neo4j": "connected"}:
                    break
            except (OSError, TimeoutError):
                pass
            time.sleep(1)
        else:
            pytest.fail("Readiness did not recover after restarting the same DB container")
        retained = _request(_entity_url(base, "11222333000181"))
        sources_after = _request(base + "/api/v1/meta/sources")
        assert sources_after[0] == 200
        restart_source_ids = [source["id"] for source in sources_after[1]["sources"]]
        restart_source_ids_sha256 = _assert_public_catalog_ids(
            restart_source_ids, expected_source_ids
        )
        restart_catalog_count = len(restart_source_ids)
        assert restart_catalog_count == len(expected_source_ids)
        evidence["restart"] = {
            "readiness": (recovered_status, recovered_body),
            "entity": retained,
            "catalog_count_after": restart_catalog_count,
            "catalog_ids_sha256_after": restart_source_ids_sha256,
        }
        assert retained[0] == 200
        assert retained[1]["properties"]["name"] == "Synthetic F10 Company"
        assert evidence["restart"]["catalog_count_after"] == catalog_count
        probe_recovered = created_api.exec_run(["/app/.venv/bin/python", script_path], demux=True)
        assert probe_recovered.exit_code == 0
    finally:
        anonymous_volume_names: list[str] = []
        if created_db is not None:
            created_db.reload()
            anonymous_volume_names = [
                mount["Name"]
                for mount in created_db.attrs.get("Mounts", [])
                if mount.get("Type") == "volume" and mount.get("Name")
            ]
        if created_api is not None:
            created_api.remove(force=True)
        if created_db is not None:
            created_db.remove(force=True, v=True)
        if created_network is not None:
            created_network.remove()
        absent: dict[str, bool] = {}
        for resource, name in names.items():
            try:
                if resource == "network":
                    client.networks.get(name)
                else:
                    client.containers.get(name)
                absent[resource] = False
            except docker.errors.NotFound:
                absent[resource] = True
        anonymous_volumes_absent: dict[str, bool] = {}
        for volume_name in anonymous_volume_names:
            try:
                client.volumes.get(volume_name)
                anonymous_volumes_absent[volume_name] = False
            except docker.errors.NotFound:
                anonymous_volumes_absent[volume_name] = True
        cleanup_evidence = {**absent, "anonymous_volumes": anonymous_volumes_absent}
        evidence["cleanup"] = cleanup_evidence
        (tmp_path / "f10-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        assert all(absent.values()), "Owned Docker resources remain after cleanup"
        assert all(anonymous_volumes_absent.values()), "Owned anonymous Docker volumes remain after cleanup"
        client.close()
