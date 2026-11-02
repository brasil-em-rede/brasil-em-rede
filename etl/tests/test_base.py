import hashlib
import json
from contextlib import suppress
from unittest.mock import MagicMock

import pytest

from bracc_etl.base import EmptyLoadEvidence, Pipeline
from bracc_etl.loader import Neo4jBatchLoader
from bracc_etl.source_aliases import UnknownSourceError


class DummyPipeline(Pipeline):
    name = "cnpj"
    source_id = "receita_federal"

    def __init__(self) -> None:
        super().__init__(driver=MagicMock(), data_dir="./data")
        self.extracted = False
        self.transformed = False
        self.loaded = False

    def extract(self) -> None:
        self.extracted = True

    def transform(self) -> None:
        self.transformed = True

    def load(self) -> None:
        self.loaded = True
        session = self.driver.session.return_value.__enter__.return_value
        tx = session.begin_transaction.return_value.__enter__.return_value
        counters = tx.run.return_value.consume.return_value.counters
        counters.nodes_created = 1
        counters.relationships_created = 0
        Neo4jBatchLoader(self.driver).load_nodes("Test", [{"id": "1"}], "id")


def test_pipeline_run_executes_all_stages() -> None:
    pipeline = DummyPipeline()
    pipeline.run()
    assert pipeline.extracted
    assert pipeline.transformed
    assert pipeline.loaded


def test_ingestion_run_stores_all_identities_without_changing_legacy_data_id() -> None:
    pipeline = DummyPipeline()
    original_run_id = pipeline.run_id
    driver = MagicMock()
    pipeline.driver = driver
    pipeline._upsert_ingestion_run(status="running")
    session = driver.session.return_value.__enter__.return_value
    query, params = session.run.call_args.args
    assert params["source_id"] == "cnpj"
    assert params["runner_name"] == "cnpj"
    assert params["provider_id"] == "receita_federal"
    assert params["source_aliases_version"] == 1
    assert params["run_id"] == original_run_id
    assert pipeline.source_id == "receita_federal"
    assert "r.provider_id = $provider_id" in query


def test_unregistered_pipeline_is_rejected() -> None:
    class UnregisteredPipeline(DummyPipeline):
        name = "unknown_source"
        source_id = "unknown_provider"

    with pytest.raises(UnknownSourceError):
        UnregisteredPipeline()


def test_empty_load_requires_evidence_and_records_failure() -> None:
    pipeline = DummyPipeline()
    pipeline.load = lambda: None
    with pytest.raises(ValueError, match="empty load requires structured extraction evidence"):
        pipeline.run()
    session = pipeline.driver.session.return_value.__enter__.return_value
    assert session.run.call_args.args[1]["status"] == "quality_fail"


def test_legacy_empty_evidence_is_not_population_proof(tmp_path) -> None:
    pipeline = DummyPipeline()
    pipeline.load = lambda: None
    raw = b"[]"
    artifact = tmp_path / "unrelated-empty.json"
    artifact.write_bytes(raw)
    source_url = "https://example.test/empty.json"
    manifest = {
        "source_id": "cnpj",
        "pages": {
            artifact.name: {
                "source_sha256": hashlib.sha256(source_url.encode()).hexdigest(),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
            }
        },
    }
    content = json.dumps(manifest).encode()
    path = tmp_path / "manifest.json"
    path.write_bytes(content)
    pipeline.empty_load_evidence = EmptyLoadEvidence(
        str(path), hashlib.sha256(content).hexdigest(), str(artifact), source_url, "json"
    )
    pipeline.configured_source_url = source_url
    pipeline.extract = lambda: setattr(pipeline, "source_rows", [{"id": "discarded"}])
    pipeline.transform = lambda: setattr(pipeline, "source_rows", [])

    with pytest.raises(ValueError, match="legacy empty evidence cannot prove"):
        pipeline.run()

    session = pipeline.driver.session.return_value.__enter__.return_value
    statuses = [call.args[1]["status"] for call in session.run.call_args_list]
    assert statuses == ["running", "quality_fail"]
    assert not pipeline.loaded


def test_empty_load_with_evidence_is_rejected_for_legacy_pipeline(tmp_path) -> None:
    pipeline = DummyPipeline()
    pipeline.load = lambda: None
    raw = b"[]"
    artifact = tmp_path / "page.json"
    artifact.write_bytes(raw)
    source_url = "https://example.test/empty.json"
    manifest = {
        "source_id": "cnpj",
        "pages": {
            artifact.name: {
                "source_sha256": hashlib.sha256(source_url.encode()).hexdigest(),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
            }
        },
    }
    content = json.dumps(manifest).encode()
    path = tmp_path / "manifest.json"
    path.write_bytes(content)
    pipeline.empty_load_evidence = EmptyLoadEvidence(
        str(path), hashlib.sha256(content).hexdigest(), str(artifact), source_url, "json"
    )
    pipeline.configured_source_url = source_url
    with pytest.raises(ValueError, match="legacy empty evidence cannot prove"):
        pipeline.run()
    session = pipeline.driver.session.return_value.__enter__.return_value
    assert session.run.call_args.args[1]["status"] == "quality_fail"
    assert pipeline.empty_load_evidence is None


def test_pipeline_does_not_succeed_when_run_status_cannot_be_persisted() -> None:
    pipeline = DummyPipeline()
    result = pipeline.driver.session.return_value.__enter__.return_value.run.return_value
    result.consume.side_effect = [None, RuntimeError("status unavailable"), None]
    with pytest.raises(RuntimeError, match="status unavailable"):
        pipeline.run()
    assert pipeline.loaded


def test_empty_manifest_hash_must_match(tmp_path) -> None:
    pipeline = DummyPipeline()
    pipeline.load = lambda: None
    path = tmp_path / "manifest.json"
    path.write_text('{"source_id":"cnpj","record_count":0}')
    pipeline.empty_load_evidence = EmptyLoadEvidence(str(path), "0" * 64)
    with pytest.raises(ValueError, match="legacy empty evidence cannot prove extracted population"):
        pipeline.run()


def test_empty_evidence_rejects_nonempty_or_tampered_artifact(tmp_path) -> None:
    raw = b"[]"
    artifact = tmp_path / "page.json"
    artifact.write_bytes(raw)
    url = "https://example.test/data"
    manifest = {
        "source_id": "cnpj",
        "pages": {
            artifact.name: {
                "source_sha256": hashlib.sha256(url.encode()).hexdigest(),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
            }
        },
    }
    contents = json.dumps(manifest).encode()
    path = tmp_path / "manifest.json"
    path.write_bytes(contents)
    evidence = EmptyLoadEvidence(
        str(path), hashlib.sha256(contents).hexdigest(), str(artifact), url, "json"
    )
    assert evidence.verify("cnpj", url)
    artifact.write_bytes(b"[1]")
    assert not evidence.verify("cnpj", url)
    assert not EmptyLoadEvidence(
        str(path),
        hashlib.sha256(contents).hexdigest(),
        str(artifact),
        "https://other.test/data",
        "json",
    ).verify("cnpj", url)


def test_empty_evidence_rejects_manifest_with_other_pages(tmp_path) -> None:
    raw = b"[]"
    artifact = tmp_path / "page-1.json"
    artifact.write_bytes(raw)
    other_raw = b"[{\"id\":1}]"
    other_artifact = tmp_path / "page-2.json"
    other_artifact.write_bytes(other_raw)
    url = "https://example.test/data"
    pages = {
        artifact.name: {
            "source_sha256": hashlib.sha256(url.encode()).hexdigest(),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "bytes": len(raw),
        },
        other_artifact.name: {
            "source_sha256": hashlib.sha256(url.encode()).hexdigest(),
            "sha256": hashlib.sha256(other_raw).hexdigest(),
            "bytes": len(other_raw),
        },
    }
    contents = json.dumps({"source_id": "cnpj", "pages": pages}).encode()
    path = tmp_path / "manifest.json"
    path.write_bytes(contents)
    evidence = EmptyLoadEvidence(
        str(path), hashlib.sha256(contents).hexdigest(), str(artifact), url, "json"
    )
    assert not evidence.verify("cnpj", url)

    pipeline = DummyPipeline()
    pipeline.load = lambda: None
    pipeline.empty_load_evidence = evidence
    pipeline.configured_source_url = url
    with pytest.raises(ValueError, match="legacy empty evidence cannot prove extracted population"):
        pipeline.run()
    session = pipeline.driver.session.return_value.__enter__.return_value
    assert session.run.call_args.args[1]["status"] == "quality_fail"


def test_untracked_direct_write_fails_closed() -> None:
    pipeline = DummyPipeline()

    def load():
        pipeline.mark_untracked_write()

    pipeline.load = load
    with pytest.raises(ValueError, match="untracked writes"):
        pipeline.run()


def test_caught_batch_failure_cannot_promote() -> None:
    pipeline = DummyPipeline()

    def load():
        session = pipeline.driver.session.return_value.__enter__.return_value
        tx = session.begin_transaction.return_value.__enter__.return_value
        tx.run.side_effect = RuntimeError("batch failure")
        with suppress(RuntimeError):
            Neo4jBatchLoader(pipeline.driver).load_nodes("Test", [{"id": 1}], "id")

    pipeline.load = load
    with pytest.raises(ValueError, match="failed batches"):
        pipeline.run()


@pytest.mark.parametrize("url", ["ftp://example.test/data.csv", "http://example.test/data.csv"])
def test_empty_csv_evidence_matches_configured_source(tmp_path, url) -> None:
    artifact = tmp_path / "page.csv"
    raw = b"id,name\n"
    artifact.write_bytes(raw)
    manifest = {
        "source_id": "cnpj",
        "pages": {
            artifact.name: {
                "source_sha256": hashlib.sha256(url.encode()).hexdigest(),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
            }
        },
    }
    content = json.dumps(manifest).encode()
    path = tmp_path / "manifest.json"
    path.write_bytes(content)
    evidence = EmptyLoadEvidence(
        str(path), hashlib.sha256(content).hexdigest(), str(artifact), url, "csv"
    )
    assert evidence.verify("cnpj", url)
    assert not evidence.verify("cnpj", "https://example.test/data.csv")


def test_reused_pipeline_resets_running_metrics() -> None:
    pipeline = DummyPipeline()
    pipeline.run()
    pipeline.run()
    session = pipeline.driver.session.return_value.__enter__.return_value
    running = [
        call.args[1] for call in session.run.call_args_list if call.args[1]["status"] == "running"
    ]
    assert len(running) == 2
    assert all(params["submitted"] == params["rows_loaded"] == 0 for params in running)


def test_direct_write_records_consumed_counters() -> None:
    pipeline = DummyPipeline()
    session = pipeline.driver.session.return_value.__enter__.return_value
    summary = session.run.return_value.consume.return_value
    counters = summary.counters
    counters.nodes_created = 2
    counters.relationships_created = 3
    counters.nodes_deleted = 4
    counters.relationships_deleted = 5
    counters.properties_set = 6
    counters.labels_added = 7
    counters.labels_removed = 8

    pipeline.run_direct_write("MATCH (n) RETURN n")

    assert pipeline.load_metrics.direct_updates == 35
    assert (
        pipeline.load_metrics.nodes_created, pipeline.load_metrics.relationships_created
    ) == (2, 3)
    assert (
        pipeline.load_metrics.nodes_deleted, pipeline.load_metrics.relationships_deleted
    ) == (4, 5)
    assert (pipeline.load_metrics.properties_set, pipeline.load_metrics.labels_added,
            pipeline.load_metrics.labels_removed) == (6, 7, 8)


def test_loader_created_in_worker_thread_uses_explicit_pipeline_run_metrics() -> None:
    from concurrent.futures import ThreadPoolExecutor

    pipeline = DummyPipeline()

    def threaded_load() -> None:
        metrics = pipeline.load_metrics
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(
                lambda: Neo4jBatchLoader(pipeline.driver, metrics=metrics).load_nodes(
                    "Test", [{"id": "threaded"}], "id"
                )
            ).result()

    pipeline.load = threaded_load
    pipeline.run()

    assert pipeline.rows_loaded == 1
    assert pipeline.load_metrics.submitted == 1
    assert pipeline.load_metrics.verified_rows == 1
    assert pipeline.load_metrics.committed_rows == 1
