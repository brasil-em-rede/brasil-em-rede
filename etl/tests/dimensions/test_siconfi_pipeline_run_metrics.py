from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from bracc_etl.dimensions import projection
from bracc_etl.dimensions.crosswalk import CrosswalkError
from bracc_etl.loader import LoadMetrics
from bracc_etl.pipelines.siconfi import SiconfiPipeline

if TYPE_CHECKING:
    from neo4j import Driver

from tests.dimensions import test_f09_metrics_boundary as f09_boundary

pytestmark = pytest.mark.integration

CSV_ROWS = (
    {
        "cod_ibge": "3550308",
        "exercicio": "2023",
        "uf": "SP",
        "versao_territorial": "f09-fixture-v1",
        "tipo_territorio": "municipio",
        "unidade_sistema": "SICONFI",
        "unidade_codigo": "BRL",
        "conta": "receita",
        "coluna": "valor",
        "valor": "100",
        "ente": "Municipio Sintetico",
        "instituicao": "Orgao Sintetico",
        "orgao_jurisdicao": "SP",
        "orgao_sistema": "synthetic",
        "orgao_papel": "ug",
        "orgao_codigo": "000123",
        "orgao_versao": "fixture-v1",
    },
    {
        "cod_ibge": "3550308",
        "exercicio": "2023",
        "uf": "SP",
        "versao_territorial": "f09-fixture-v1",
        "tipo_territorio": "municipio",
        "unidade_sistema": "SICONFI",
        "unidade_codigo": "BRL",
        "conta": "despesa",
        "coluna": "valor",
        "valor": "200",
        "ente": "Municipio Sintetico",
        "instituicao": "Orgao Sintetico",
        "orgao_jurisdicao": "SP",
        "orgao_sistema": "synthetic",
        "orgao_papel": "ug",
        "orgao_codigo": "000123",
        "orgao_versao": "fixture-v1",
    },
)

CROSSWALK_ENTRIES = [
    {
        "territory": {
            "system": "IBGE",
            "type": "municipio",
            "code": "3550308",
            "territorial_version": "f09-fixture-v1",
            "uf": "SP",
        },
        "public_body": {
            "jurisdiction": "SP",
            "system": "synthetic",
            "role": "ug",
            "code": "000123",
            "version": "fixture-v1",
        },
        "unit": {"system": "SICONFI", "code": "BRL"},
        "fiscal_year": {"year": "2023"},
    }
]


def _crosswalk(entries: list[dict[str, Any]]) -> dict[str, Any]:
    canonical = json.dumps(entries, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return {
        "provenance": {
            "source": "fixture-sintetica-f09-siconfi",
            "version": "fixture-v1",
            "artifact_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        },
        "entries": entries,
    }


def _write_csv(data_dir: Path) -> Path:
    source_dir = data_dir / "siconfi"
    source_dir.mkdir(parents=True)
    csv_path = source_dir / "dca_fixture.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(CSV_ROWS[0]))
        writer.writeheader()
        writer.writerows(CSV_ROWS)
    return csv_path


def _identity(properties: dict[str, Any]) -> str:
    for field in (
        "finance_id",
        "territory_id",
        "public_body_id",
        "unit_id",
        "fiscal_year_id",
    ):
        if field in properties:
            return str(properties[field])
    raise AssertionError(f"No identity property in {properties!r}")


def _snapshot(
    graph_driver: Driver, database: str
) -> tuple[dict[tuple[str, str], dict[str, Any]], set[tuple[Any, ...]]]:
    with graph_driver.session(database=database) as session:
        nodes = session.run(
            "MATCH (n) WHERE NOT n:IngestionRun "
            "RETURN labels(n) AS labels, properties(n) AS properties"
        ).data()
        relations = session.run(
            "MATCH (s)-[r]->(t) "
            "RETURN labels(s) AS source_labels, properties(s) AS source, type(r) AS kind, "
            "labels(t) AS target_labels, properties(t) AS target, properties(r) AS relation"
        ).data()
    node_map = {
        (record["labels"][0], _identity(record["properties"])): record["properties"]
        for record in nodes
    }
    relation_set = {
        (
            record["source_labels"][0],
            _identity(record["source"]),
            record["kind"],
            record["target_labels"][0],
            _identity(record["target"]),
            tuple(sorted(record["relation"].items())),
        )
        for record in relations
    }
    return node_map, relation_set


def _runs(graph_driver: Driver, database: str) -> dict[str, dict[str, Any]]:
    with graph_driver.session(database=database) as session:
        records = session.run(
            "MATCH (r:IngestionRun) RETURN r.run_id AS run_id, properties(r) AS properties"
        ).data()
    return {record["run_id"]: record["properties"] for record in records}


def _metrics(pipeline: SiconfiPipeline) -> dict[str, int]:
    assert isinstance(pipeline.load_metrics, LoadMetrics)
    return {
        field: getattr(pipeline.load_metrics, field)
        for field in (
            "failed_batches",
            "direct_updates",
            "untracked_writes",
            "submitted",
            "attempts",
            "rejected",
            "committed_rows",
            "verified_rows",
            "generic_updates",
            "nodes_created",
            "relationships_created",
            "nodes_deleted",
            "relationships_deleted",
            "properties_set",
            "labels_added",
            "labels_removed",
            "relationships_resolved",
            "unresolved",
        )
    }


def _assert_run_receipt(
    receipt: dict[str, Any],
    *,
    run_id: str,
    metrics: dict[str, Any],
    status: str,
    rows_loaded: int,
    error: str | None = None,
) -> None:
    assert receipt["run_id"] == run_id
    assert receipt["status"] == status
    assert receipt["source_id"] == "siconfi"
    assert receipt["runner_name"] == "siconfi"
    assert receipt["provider_id"] == "siconfi"
    assert receipt["source_aliases_version"] == 1
    assert receipt["rows_in"] == 0
    assert receipt["rows_loaded"] == rows_loaded
    assert receipt["verified_rows"] == rows_loaded
    for field, expected in metrics.items():
        assert receipt[field] == expected
    assert receipt["started_at"]
    if status == "loaded":
        assert receipt["finished_at"]
        assert receipt.get("error") is None
    else:
        assert receipt["finished_at"]
        assert receipt["error"] == error

siconfi_graph_driver = f09_boundary.driver



def test_siconfi_run_persists_dimension_metrics_replay_and_crosswalk_refusal(
    siconfi_graph_driver: Driver, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = "neo4j"
    monkeypatch.setenv("NEO4J_DATABASE", database)
    csv_path = _write_csv(tmp_path)
    assert csv_path.is_file()
    crosswalk = _crosswalk(CROSSWALK_ENTRIES)
    pipeline = SiconfiPipeline(
        siconfi_graph_driver,
        data_dir=str(tmp_path),
        dimension_crosswalk=crosswalk,
    )

    pipeline_module = __import__(SiconfiPipeline.__module__, fromlist=["__file__"])
    source_paths = {
        "pipeline": Path(pipeline_module.__file__).resolve(),
        "projector": Path(projection.__file__).resolve(),
    }
    assert source_paths["pipeline"].is_relative_to(Path(__file__).resolve().parents[2] / "src")
    assert source_paths["projector"].is_relative_to(Path(__file__).resolve().parents[2] / "src")
    assert pipeline.run.__func__.__code__.co_filename == str(
        (Path(__file__).resolve().parents[2] / "src/bracc_etl/base.py").resolve()
    )
    assert pipeline.extract.__func__.__code__.co_filename == str(
        (Path(__file__).resolve().parents[2] / "src/bracc_etl/pipelines/siconfi.py").resolve()
    )
    assert pipeline.transform.__func__.__code__.co_filename == str(
        (Path(__file__).resolve().parents[2] / "src/bracc_etl/pipelines/siconfi.py").resolve()
    )
    assert pipeline.load.__func__.__code__.co_filename == str(
        (Path(__file__).resolve().parents[2] / "src/bracc_etl/pipelines/siconfi.py").resolve()
    )
    assert projection.project_siconfi.__code__.co_filename == str(
        (Path(__file__).resolve().parents[2] / "src/bracc_etl/dimensions/projection.py").resolve()
    )

    first_metrics = {
        "failed_batches": 0,
        "direct_updates": 41,
        "untracked_writes": 0,
        "submitted": 2,
        "attempts": 1,
        "rejected": 0,
        "committed_rows": 2,
        "verified_rows": 4,
        "generic_updates": 0,
        "nodes_created": 6,
        "relationships_created": 8,
        "nodes_deleted": 0,
        "relationships_deleted": 0,
        "properties_set": 59,
        "labels_added": 6,
        "labels_removed": 0,
        "relationships_resolved": 0,
        "unresolved": 0,
    }
    pipeline.run()
    first_run_id = pipeline.run_id
    assert _metrics(pipeline) == first_metrics
    first_nodes, first_relations = _snapshot(siconfi_graph_driver, database)
    assert first_nodes == {
        ("MunicipalFinance", "ea16d71178b737dc"): {
            "finance_id": "ea16d71178b737dc",
            "cod_ibge": "3550308",
            "municipality": "MUNICIPIO SINTETICO",
            "reporting_institution": "ORGAO SINTETICO",
            "year": "2023",
            "account": "receita",
            "column": "valor",
            "amount": 100.0,
            "source": "siconfi",
            "code_validation_status": "matched_local",
            "code_validation_reason": "",
            "dimension_status": "projected",
            "dimension_reason": "",
            "territory_unit_validation_status": "matched_local",
            "territory_unit_validation_reason": "",
            "public_body_validation_status": "matched_local",
            "public_body_validation_reason": "",
        },
        ("MunicipalFinance", "3bf4b4006273cb29"): {
            "finance_id": "3bf4b4006273cb29",
            "cod_ibge": "3550308",
            "municipality": "MUNICIPIO SINTETICO",
            "reporting_institution": "ORGAO SINTETICO",
            "year": "2023",
            "account": "despesa",
            "column": "valor",
            "amount": 200.0,
            "source": "siconfi",
            "code_validation_status": "matched_local",
            "code_validation_reason": "",
            "dimension_status": "projected",
            "dimension_reason": "",
            "territory_unit_validation_status": "matched_local",
            "territory_unit_validation_reason": "",
            "public_body_validation_status": "matched_local",
            "public_body_validation_reason": "",
        },
        ("Territory", "IBGE:municipio:3550308:f09-fixture-v1"): {
            "territory_id": "IBGE:municipio:3550308:f09-fixture-v1",
            "system": "IBGE",
            "type": "municipio",
            "code": "3550308",
            "territorial_version": "f09-fixture-v1",
            "uf": "SP",
        },
        ("PublicBody", "SP:synthetic:ug:000123:fixture-v1"): {
            "public_body_id": "SP:synthetic:ug:000123:fixture-v1",
            "jurisdiction": "SP",
            "system": "synthetic",
            "role": "ug",
            "code": "000123",
            "version": "fixture-v1",
        },
        ("Unit", "SICONFI:BRL"): {
            "unit_id": "SICONFI:BRL",
            "system": "SICONFI",
            "code": "BRL",
        },
        ("FiscalYear", "2023"): {"fiscal_year_id": "2023", "year": "2023"},
    }
    territory_id = "IBGE:municipio:3550308:f09-fixture-v1"
    body_id = "SP:synthetic:ug:000123:fixture-v1"
    expected_relations = {
        (
            "MunicipalFinance",
            fact_id,
            relation,
            label,
            identity,
            (),
        )
        for fact_id in ("ea16d71178b737dc", "3bf4b4006273cb29")
        for relation, label, identity in (
            ("IN_TERRITORY", "Territory", territory_id),
            ("REFERENCES_BODY", "PublicBody", body_id),
            ("MEASURED_IN", "Unit", "SICONFI:BRL"),
            ("IN_FISCAL_YEAR", "FiscalYear", "2023"),
        )
    }
    assert first_relations == expected_relations

    assert not any(label in {"Person", "Company"} for label, _ in first_nodes)
    first_run = _runs(siconfi_graph_driver, database)
    assert set(first_run) == {first_run_id}
    first_receipt = first_run[first_run_id]
    _assert_run_receipt(
        first_receipt,
        run_id=first_run_id,
        metrics=first_metrics,
        status="loaded",
        rows_loaded=4,
    )
    assert first_receipt["direct_updates"] == 41
    assert first_receipt["properties_set"] == 59
    assert first_receipt["verified_rows"] == 4

    replay_metrics = {
        "failed_batches": 0,
        "direct_updates": 4,
        "untracked_writes": 0,
        "submitted": 2,
        "attempts": 1,
        "rejected": 0,
        "committed_rows": 2,
        "verified_rows": 4,
        "generic_updates": 0,
        "nodes_created": 0,
        "relationships_created": 0,
        "nodes_deleted": 0,
        "relationships_deleted": 0,
        "properties_set": 36,
        "labels_added": 0,
        "labels_removed": 0,
        "relationships_resolved": 0,
        "unresolved": 0,
    }
    pipeline.run()
    replay_run_id = pipeline.run_id
    assert replay_run_id != first_run_id
    assert _metrics(pipeline) == replay_metrics
    second_nodes, second_relations = _snapshot(siconfi_graph_driver, database)
    assert second_nodes == first_nodes
    assert second_relations == first_relations
    assert second_relations == expected_relations
    runs_after_replay = _runs(siconfi_graph_driver, database)
    assert set(runs_after_replay) == {first_run_id, replay_run_id}
    assert runs_after_replay[first_run_id] == first_receipt
    replay_receipt = runs_after_replay[replay_run_id]
    _assert_run_receipt(
        replay_receipt,
        run_id=replay_run_id,
        metrics=replay_metrics,
        status="loaded",
        rows_loaded=4,
    )
    assert replay_receipt["direct_updates"] == 4
    assert replay_receipt["properties_set"] == 36
    assert replay_receipt["verified_rows"] == 4

    invalid_crosswalk = {
        **crosswalk,
        "provenance": {**crosswalk["provenance"], "artifact_sha256": "0" * 64},
    }
    pipeline.dimension_crosswalk = invalid_crosswalk
    with pytest.raises(
        CrosswalkError, match="^artifact_sha256 does not match canonical entries$"
    ) as raised:
        pipeline.run()
    failure_run_id = pipeline.run_id
    assert failure_run_id not in {first_run_id, replay_run_id}
    assert str(raised.value) == "artifact_sha256 does not match canonical entries"
    failure_metrics = {
        "failed_batches": 0,
        "direct_updates": 0,
        "untracked_writes": 0,
        "submitted": 0,
        "attempts": 0,
        "rejected": 0,
        "committed_rows": 0,
        "verified_rows": 0,
        "generic_updates": 0,
        "nodes_created": 0,
        "relationships_created": 0,
        "nodes_deleted": 0,
        "relationships_deleted": 0,
        "properties_set": 0,
        "labels_added": 0,
        "labels_removed": 0,
        "relationships_resolved": 0,
        "unresolved": 0,
    }
    assert _metrics(pipeline) == failure_metrics
    assert _snapshot(siconfi_graph_driver, database) == (first_nodes, first_relations)
    runs_after_failure = _runs(siconfi_graph_driver, database)
    assert set(runs_after_failure) == {first_run_id, replay_run_id, failure_run_id}
    assert runs_after_failure[first_run_id] == first_receipt
    assert runs_after_failure[replay_run_id] == replay_receipt
    _assert_run_receipt(
        runs_after_failure[failure_run_id],
        run_id=failure_run_id,
        metrics=failure_metrics,
        status="quality_fail",
        rows_loaded=0,
        error="artifact_sha256 does not match canonical entries",
    )
    assert runs_after_failure[failure_run_id]["direct_updates"] == 0
    assert runs_after_failure[failure_run_id]["properties_set"] == 0
    assert runs_after_failure[failure_run_id]["verified_rows"] == 0
    with siconfi_graph_driver.session(database=database) as session:
        counts = session.run(
            "MATCH (n) WITH count(n) AS nodes "
            "OPTIONAL MATCH ()-[r]->() RETURN nodes, count(r) AS relationships"
        ).single(strict=True)
    assert counts["nodes"] == 9
    assert counts["relationships"] == 8
