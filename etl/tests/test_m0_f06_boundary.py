from __future__ import annotations

import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from neo4j import Driver, GraphDatabase
from neo4j.exceptions import TransientError

from bracc_etl.acquisition.client import Acquisition, Manifest
from bracc_etl.acquisition.extraction_bridge import (
    build_extraction_manifest,
    open_acquired_extraction,
)
from bracc_etl.base import Pipeline
from bracc_etl.extraction import (
    ExtractionContractError,
    FilterPlan,
    InputPlan,
    PopulationSpec,
    SourceAdapterSpec,
    csv_dict_parser,
)
from bracc_etl.loader import Neo4jBatchLoader

if TYPE_CHECKING:
    from pathlib import Path

SOURCE = "cvm_funds"


def _adapter(url: str) -> SourceAdapterSpec:
    population = PopulationSpec(
        "funds",
        "synthetic-funds",
        "1",
        (("funds-csv", "csv"),),
        csv_dict_parser,
        ("fund_id", "name"),
    )
    return SourceAdapterSpec("synthetic-funds", "1", SOURCE, url, 1, (population,))


class SyntheticFundsPipeline(Pipeline):
    name = "cvm_funds"
    source_id = SOURCE

    def __init__(
        self,
        driver: Driver,
        root: Path,
        url: str,
        *,
        filtered: bool = False,
        relationships: bool = False,
    ) -> None:
        super().__init__(driver, data_dir=str(root), chunk_size=2)
        self.root = root
        self.adapter = _adapter(url)
        self.extraction_adapter = self.adapter
        self.configured_source_url = url
        self.filtered = filtered
        self.relationships = relationships

    def extract(self) -> None:
        raise AssertionError("structured extraction must replace legacy extract")

    def transform(self) -> None:
        raise AssertionError("structured extraction must replace legacy transform")

    def load(self) -> None:
        raise AssertionError("structured extraction must replace legacy load")

    def produce_extraction(self) -> tuple[Any, tuple[Any, ...]]:
        manifest = Manifest.open(self.root / "acquisition.json", SOURCE, "test")
        url = self.configured_source_url
        assert isinstance(url, str)
        path = self.root / f"f06-extraction-manifest-{self.run_id}.json"
        path = build_extraction_manifest(
            self.root,
            manifest,
            self.adapter,
            run_id=self.run_id,
            source_id=SOURCE,
            source_url=url,
            assignments={"funds": {"funds-csv": ("funds.csv", "synthetic-funds", "1")}},
            page_urls={"funds.csv": url},
            output_path=path,
        )
        return open_acquired_extraction(
            self.root,
            path,
            self.adapter,
            run_id=self.run_id,
            source_id=SOURCE,
            source_url=url,
        )

    def transform_result(self, result: Any) -> Any:
        source = InputPlan(result)
        if self.filtered:
            return FilterPlan(source, "fund_id", "eq", "never")
        return source

    def load_extraction(self, records: Any) -> None:
        rows = list(records)
        self.rows_in = len(rows)
        loader = Neo4jBatchLoader(self.driver, batch_size=2, metrics=self.load_metrics)
        loader.load_nodes(
            "F06Funds", [{"fund_id": r["fund_id"], "name": r["name"]} for r in rows], "fund_id"
        )
        if self.relationships and rows:
            loader.load_relationships(
                "RELATED_TO",
                [
                    {
                        "source_key": rows[0]["fund_id"],
                        "target_key": rows[1]["fund_id"],
                        "kind": "synthetic",
                    },
                    {
                        "source_key": rows[1]["fund_id"],
                        "target_key": rows[2]["fund_id"],
                        "kind": "synthetic",
                    },
                    {
                        "source_key": rows[0]["fund_id"],
                        "target_key": "missing-endpoint",
                        "kind": "unresolved",
                    },
                    {"source_key": "", "target_key": rows[0]["fund_id"], "kind": "rejected"},
                ],
                "F06Funds",
                "fund_id",
                "F06Funds",
                "fund_id",
                properties=["kind"],
            )


def _serve(body: bytes) -> tuple[ThreadingHTTPServer, Thread, str, list[int]]:
    requests = [0]

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests[0] += 1
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", '"synthetic-v1"')
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_port}/synthetic.csv", requests


def _stop(server: ThreadingHTTPServer, thread: Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)
    assert not thread.is_alive()


def _acquire(root: Path, body: bytes) -> tuple[ThreadingHTTPServer, Thread, str]:
    server, thread, url, _requests = _serve(body)
    manifest = Manifest.open(root / "acquisition.json", SOURCE, "test")
    Acquisition(retries=1, backoff=0, timeout=5).acquire_pages(
        manifest, [("funds.csv", url)], root, "csv"
    )
    return server, thread, url


def _driver() -> Driver:
    uri = os.environ.get("BRACC_TEST_NEO4J_URI")
    if not uri:
        pytest.skip("requires run-owned disposable Neo4j")
    driver = GraphDatabase.driver(uri, auth=("neo4j", os.environ["BRACC_TEST_NEO4J_PASSWORD"]))
    driver.verify_connectivity()
    return driver


def _run(
    tmp_path: Path,
    driver: Driver,
    body: bytes,
    *,
    filtered: bool = False,
    relationships: bool = False,
) -> SyntheticFundsPipeline:
    server, thread, url = _acquire(tmp_path, body)
    try:
        pipeline = SyntheticFundsPipeline(
            driver, tmp_path, url, filtered=filtered, relationships=relationships
        )
        pipeline.run()
        return pipeline
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


@pytest.mark.integration
def test_http_f05_to_pipeline_to_neo4j_persists_metrics_and_replay(tmp_path: Path) -> None:
    driver = _driver()
    prefix = uuid4().hex
    body = f"fund_id,name\n{prefix}-1,Alpha\n{prefix}-2,Beta\n{prefix}-3,Gamma\n".encode()
    ids: list[str] = []
    server = None
    try:
        server, thread, url, requests = _serve(body)
        manifest = Manifest.open(tmp_path / "acquisition.json", SOURCE, "test")
        Acquisition(retries=1, backoff=0, timeout=5).acquire_pages(
            manifest, [("funds.csv", url)], tmp_path, "csv"
        )
        first = SyntheticFundsPipeline(driver, tmp_path, url, relationships=True)
        first.run()
        ids.append(first.run_id)
        _stop(server, thread)
        server = None
        second = SyntheticFundsPipeline(driver, tmp_path, url, relationships=True)
        second.run()
        ids.append(second.run_id)
        assert requests[0] == 1
        assert first.rows_loaded == second.rows_loaded == 5
        assert first.load_metrics.nodes_created == 3
        assert second.load_metrics.nodes_created == 0
        with driver.session() as session:
            nodes = list(
                session.run(
                    "MATCH (n:F06Funds) WHERE n.fund_id STARTS WITH $prefix "
                    "RETURN n.fund_id AS id, n.name AS name ORDER BY id",
                    prefix=prefix,
                )
            )
            assert [(row["id"], row["name"]) for row in nodes] == [
                (f"{prefix}-1", "Alpha"),
                (f"{prefix}-2", "Beta"),
                (f"{prefix}-3", "Gamma"),
            ]
            edges = list(
                session.run(
                    "MATCH (a:F06Funds)-[r:RELATED_TO]->(b:F06Funds) "
                    "WHERE a.fund_id STARTS WITH $prefix "
                    "RETURN a.fund_id AS source, b.fund_id AS target, r.kind AS kind "
                    "ORDER BY source, target",
                    prefix=prefix,
                )
            )
            assert [(r["source"], r["target"], r["kind"]) for r in edges] == [
                (f"{prefix}-1", f"{prefix}-2", "synthetic"),
                (f"{prefix}-2", f"{prefix}-3", "synthetic"),
            ]
            records = list(
                session.run(
                    "MATCH (r:IngestionRun) WHERE r.run_id IN $ids "
                    "RETURN r.run_id AS run_id, r.status AS status, r.rows_in AS rows_in, "
                    "r.rows_loaded AS rows_loaded, r.submitted AS submitted, "
                    "r.attempts AS attempts, "
                    "r.rejected AS rejected, r.committed_rows AS committed_rows, "
                    "r.nodes_created AS nodes_created, "
                    "r.relationships_created AS relationships_created, "
                    "r.failed_batches AS failed_batches, r.unresolved AS unresolved",
                    ids=ids,
                )
            )
            assert {r["run_id"] for r in records} == set(ids)
            assert len(records) == 2
            assert all(r["status"] == "loaded" for r in records)
            assert all(r["rows_in"] == 3 and r["rows_loaded"] == 5 for r in records)
            assert all(r["submitted"] == 7 and r["rejected"] == 1 for r in records)
            assert all(r["attempts"] == 4 and r["unresolved"] == 1 for r in records)
            assert sum(r["nodes_created"] for r in records) == 3
            assert sum(r["relationships_created"] for r in records) == 2
            assert all(r["committed_rows"] == 6 and r["failed_batches"] == 0 for r in records), [
                r.data() for r in records
            ]
    finally:
        if server is not None:
            _stop(server, thread)
        with driver.session() as session:
            session.run(
                "MATCH (n:F06Funds) WHERE n.fund_id STARTS WITH $prefix DETACH DELETE n",
                prefix=prefix,
            ).consume()
            session.run(
                "MATCH (r:IngestionRun) WHERE r.run_id IN $ids DETACH DELETE r", ids=ids
            ).consume()
        driver.close()


@pytest.mark.integration
def test_filtered_zero_is_accepted_by_verified_plan_and_persisted(tmp_path: Path) -> None:
    driver = _driver()
    ids: list[str] = []
    try:
        body = f"fund_id,name\n{uuid4().hex}-1,Alpha\n".encode()
        pipeline = _run(tmp_path, driver, body, filtered=True)
        ids.append(pipeline.run_id)
        assert pipeline.rows_loaded == 0
        assert pipeline.load_metrics.submitted == 0
        with driver.session() as session:
            row = session.run(
                "MATCH (r:IngestionRun {run_id: $id}) "
                "RETURN r.status AS status, r.rows_in AS rows_in, r.rows_loaded AS rows_loaded, "
                "r.submitted AS submitted, r.committed_rows AS committed_rows",
                id=pipeline.run_id,
            ).single(strict=True)
            assert row.data() == {
                "status": "loaded",
                "rows_in": 0,
                "rows_loaded": 0,
                "submitted": 0,
                "committed_rows": 0,
            }
    finally:
        with driver.session() as session:
            session.run(
                "MATCH (r:IngestionRun) WHERE r.run_id IN $ids DETACH DELETE r", ids=ids
            ).consume()
        driver.close()


@pytest.mark.integration
def test_invalid_filter_field_is_refused_before_load_and_persisted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = _driver()
    fund_id = f"{uuid4().hex}-1"
    server, thread, url = _acquire(tmp_path, f"fund_id,name\n{fund_id},Alpha\n".encode())
    pipeline = SyntheticFundsPipeline(driver, tmp_path, url)
    load_calls: list[int] = []
    original_load = pipeline.load_extraction

    def track_load(records: Any) -> None:
        load_calls.append(1)
        original_load(records)

    monkeypatch.setattr(pipeline, "load_extraction", track_load)
    monkeypatch.setattr(
        pipeline,
        "transform_result",
        lambda result: FilterPlan(InputPlan(result), "missing_field", "eq", "x"),
    )
    try:
        with pytest.raises(ExtractionContractError) as refusal:
            pipeline.run()
        assert str(refusal.value) == "filter field is missing"
        assert load_calls == []
        with driver.session() as session:
            row = session.run(
                "MATCH (r:IngestionRun {run_id:$id}) "
                "RETURN r.status AS status,r.submitted AS submitted",
                id=pipeline.run_id,
            ).single(strict=True)
            assert row.data() == {"status": "quality_fail", "submitted": 0}
            assert (
                session.run(
                    "MATCH (n:F06Funds {fund_id: $fund_id}) RETURN count(n) AS count",
                    fund_id=fund_id,
                ).single(strict=True)["count"]
                == 0
            )
    finally:
        _stop(server, thread)
        with driver.session() as session:
            session.run(
                "MATCH (n:F06Funds {fund_id: $fund_id}) DETACH DELETE n",
                fund_id=fund_id,
            ).consume()
            session.run(
                "MATCH (r:IngestionRun {run_id:$id}) DETACH DELETE r", id=pipeline.run_id
            ).consume()
        driver.close()


@pytest.mark.integration
def test_valid_header_only_csv_runs_pipeline_and_persists_true_zero(tmp_path: Path) -> None:
    driver = _driver()
    pipeline: SyntheticFundsPipeline | None = None
    try:
        pipeline = _run(tmp_path, driver, b"fund_id,name\n")
        assert pipeline.rows_loaded == 0
        assert pipeline.load_metrics.submitted == 0
        with driver.session() as session:
            row = session.run(
                "MATCH (r:IngestionRun {run_id: $id}) "
                "RETURN r.status AS status, r.rows_in AS rows_in, r.rows_loaded AS rows_loaded, "
                "r.submitted AS submitted, r.committed_rows AS committed_rows",
                id=pipeline.run_id,
            ).single(strict=True)
            assert row.data() == {
                "status": "loaded",
                "rows_in": 0,
                "rows_loaded": 0,
                "submitted": 0,
                "committed_rows": 0,
            }
    finally:
        if pipeline:
            with driver.session() as session:
                session.run(
                    "MATCH (r:IngestionRun {run_id: $id}) DETACH DELETE r", id=pipeline.run_id
                ).consume()
        driver.close()


@pytest.mark.integration
@pytest.mark.parametrize("retry_exhaustion", [False, True])
def test_final_batch_failure_and_retry_exhaustion_preserve_partial_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, retry_exhaustion: bool
) -> None:

    driver = _driver()
    prefix = uuid4().hex
    body = ("fund_id,name\n" + "".join(f"{prefix}-{i},Fund{i}\n" for i in range(5))).encode()
    run_ids: list[str] = []
    original = Neo4jBatchLoader._run_batch_once
    calls = 0

    def fail_final_batch(
        self: Neo4jBatchLoader, query: str, batch: list[dict[str, Any]], relationship: bool = False
    ) -> tuple[Any, int]:
        nonlocal calls
        calls += 1
        if batch and batch[0].get("fund_id") == f"{prefix}-4":
            if retry_exhaustion:
                raise TransientError("Synthetic injected transient at loader transaction boundary")
            raise ValueError("Synthetic deterministic final-batch failure")
        return original(self, query, batch, relationship)

    monkeypatch.setattr(Neo4jBatchLoader, "_run_batch_once", fail_final_batch)
    if retry_exhaustion:
        monkeypatch.setattr("bracc_etl.loader.time.sleep", lambda _seconds: None)
    try:
        server, thread, url = _acquire(tmp_path, body)
        try:
            pipeline = SyntheticFundsPipeline(driver, tmp_path, url)
            run_id = pipeline.run_id
            run_ids.append(run_id)
            error_type = TransientError if retry_exhaustion else ValueError
            error_message = (
                "Synthetic injected transient"
                if retry_exhaustion
                else "Synthetic deterministic final-batch failure"
            )
            with pytest.raises(error_type, match=error_message):
                pipeline.run()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            assert not thread.is_alive()
        assert calls == (7 if retry_exhaustion else 3)
        run_id = pipeline.run_id
        with driver.session() as session:
            nodes = list(
                session.run(
                    "MATCH (n:F06Funds) WHERE n.fund_id STARTS WITH $prefix "
                    "RETURN n.fund_id AS id ORDER BY id",
                    prefix=prefix,
                )
            )
            assert [r["id"] for r in nodes] == [
                f"{prefix}-0",
                f"{prefix}-1",
                f"{prefix}-2",
                f"{prefix}-3",
            ]
            row = session.run(
                "MATCH (r:IngestionRun {run_id: $id}) "
                "RETURN r.status AS status, r.rows_in AS rows_in, r.rows_loaded AS rows_loaded, "
                "r.submitted AS submitted, r.attempts AS attempts, "
                "r.committed_rows AS committed_rows, "
                "r.nodes_created AS nodes_created, r.failed_batches AS failed_batches",
                id=run_id,
            ).single(strict=True)
            assert row.data() == {
                "status": "quality_fail",
                "rows_in": 5,
                "rows_loaded": 4,
                "submitted": 5,
                "attempts": 7 if retry_exhaustion else 3,
                "committed_rows": 4,
                "nodes_created": 4,
                "failed_batches": 1,
            }
    finally:
        with driver.session() as session:
            session.run(
                "MATCH (n:F06Funds) WHERE n.fund_id STARTS WITH $prefix DETACH DELETE n",
                prefix=prefix,
            ).consume()
            session.run(
                "MATCH (r:IngestionRun) WHERE r.run_id IN $ids DETACH DELETE r", ids=run_ids
            ).consume()
        driver.close()


@pytest.mark.integration
@pytest.mark.parametrize("mutation", ["same-length-digest", "declared-length-after-bridge"])
def test_manifest_integrity_failure_reaches_f06_and_prevents_load(
    tmp_path: Path, mutation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = _driver()
    fund_id = f"{uuid4().hex}-1"
    body = f"fund_id,name\n{fund_id},Alpha\n".encode()
    pipeline: SyntheticFundsPipeline | None = None
    try:
        server, thread, url = _acquire(tmp_path, body)
        try:
            pipeline = SyntheticFundsPipeline(driver, tmp_path, url)
            load_calls: list[int] = []
            original_load = pipeline.load_extraction

            def track_load(records: Any) -> None:
                load_calls.append(1)
                original_load(records)

            monkeypatch.setattr(pipeline, "load_extraction", track_load)
            original_produce = pipeline.produce_extraction
            saved: dict[str, Any] = {}

            def corrupted_produce() -> tuple[Any, tuple[Any, ...]]:
                result, cursors = original_produce()
                saved["cursors"] = cursors
                if mutation == "same-length-digest":
                    path = tmp_path / "funds.csv"
                    content = path.read_bytes()
                    path.write_bytes(content.replace(b"Alpha", b"Omega"))
                else:
                    cursor = cursors[0]
                    original_length = cursor.binding.byte_length
                    assert len((tmp_path / "funds.csv").read_bytes()) == original_length
                    # TEST-ONLY metadata fault after bridge/open; raw bytes and digest stay valid.
                    object.__setattr__(cursor.binding, "byte_length", original_length + 1)
                return result, cursors

            def tracked_produce() -> tuple[Any, tuple[Any, ...]]:
                result, cursors = corrupted_produce()
                for cursor in cursors:
                    close = cursor.close

                    def tracked_close(close: Any = close) -> None:
                        close()
                        saved["closed"] = True

                    cursor.close = tracked_close
                return result, cursors

            monkeypatch.setattr(pipeline, "produce_extraction", tracked_produce)
            expected_message = (
                "artifact digest mismatch"
                if mutation == "same-length-digest"
                else "artifact byte length mismatch"
            )
            with pytest.raises(ExtractionContractError) as refusal:
                pipeline.run()
            assert str(refusal.value) == expected_message
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            assert not thread.is_alive()
        assert saved["closed"]
        assert load_calls == []
        assert all(cursor.state == "falha" for cursor in saved["cursors"])
        assert all(cursor._reader.closed for cursor in saved["cursors"])
        with driver.session() as session:
            assert (
                session.run(
                    "MATCH (n:F06Funds {fund_id: $fund_id}) RETURN count(n) AS count",
                    fund_id=fund_id,
                ).single(strict=True)["count"]
                == 0
            )
            row = session.run(
                "MATCH (r:IngestionRun {run_id: $id}) "
                "RETURN r.status AS status, r.rows_loaded AS loaded, "
                "r.submitted AS submitted",
                id=pipeline.run_id,
            ).single(strict=True)
            assert row["status"] == "quality_fail"
            assert row["loaded"] == row["submitted"] == 0
    finally:
        if pipeline:
            with driver.session() as session:
                session.run(
                    "MATCH (n:F06Funds {fund_id: $fund_id}) DETACH DELETE n",
                    fund_id=fund_id,
                ).consume()
                session.run(
                    "MATCH (r:IngestionRun {run_id: $id}) DETACH DELETE r", id=pipeline.run_id
                ).consume()
        driver.close()
