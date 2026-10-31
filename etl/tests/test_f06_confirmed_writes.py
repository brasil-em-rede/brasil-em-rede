from __future__ import annotations

from threading import Event, Thread
from typing import Any
from unittest.mock import MagicMock

import pytest

from bracc_etl.base import Pipeline
from bracc_etl.dimensions.projection import project_rais, project_siconfi
from bracc_etl.loader import LoadMetrics, Neo4jBatchLoader

persisted_statuses: list[str] = []


class Counters:
    nodes_created = 1
    relationships_created = 1
    nodes_deleted = relationships_deleted = properties_set = 0
    labels_added = labels_removed = 0


class Tx:
    def __enter__(self) -> Tx:
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def commit(self) -> None:
        return None

    def run(self, query: str, *args: Any, **params: Any) -> Any:
        if args:
            params.update(args[0])
        result = MagicMock()
        if "RETURN n" in query:
            result.single.return_value = {"n": params["props"]}
        else:
            result.single.return_value = {"matched": 1}
        result.consume.return_value.counters = Counters()
        return result


class Session:
    def __enter__(self) -> Session:
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def execute_write(self, callback: Any) -> Any:
        return callback(Tx())

    def begin_transaction(self) -> Tx:
        return Tx()

    def run(self, query: str, params: dict[str, Any]) -> Any:
        persisted_statuses.append(params["status"])
        result = MagicMock()
        result.consume.return_value.counters = Counters()
        return result


class SyntheticPipeline(Pipeline):
    name = "rais"
    source_id = "rais_mte"

    def extract(self) -> None:
        return None

    def transform(self) -> None:
        return None

    def load(self) -> None:
        return None


def fixture() -> tuple[SyntheticPipeline, Any]:
    driver = MagicMock()
    driver.session.side_effect = lambda **kwargs: Session()
    return SyntheticPipeline(driver), driver


def observation(source: str) -> list[dict[str, Any]]:
    key = "stats_id" if source == "rais" else "finance_id"
    return [{key: "fact-1", "dimensions": {"territory": {
        "system": "IBGE", "type": "uf", "code": "35",
        "territorial_version": "v1", "uf": "SP",
    }}}]


@pytest.mark.parametrize("source,project", [("rais", project_rais), ("siconfi", project_siconfi)])
def test_projection_records_confirmed_writes_inside_pipeline_run(source: str, project: Any) -> None:
    persisted_statuses.clear()
    pipeline, driver = fixture()
    def load() -> None:
        project(driver, "neo4j", observation(source))
    pipeline.load = load  # type: ignore[method-assign]
    pipeline.run()
    expected_updates = 4 if source == "rais" else 6
    assert pipeline.load_metrics.direct_updates == expected_updates
    assert pipeline.load_metrics.nodes_created == (2 if source == "rais" else 3)
    assert pipeline.load_metrics.relationships_created == (2 if source == "rais" else 3)
    assert pipeline.load_metrics.untracked_writes == 0
    assert persisted_statuses == ["running", "loaded"]


def test_projection_failure_records_untracked_and_persists_quality_fail() -> None:
    persisted_statuses.clear()
    pipeline, driver = fixture()
    class MissingTx(Tx):
        def run(self, query: str, *args: Any, **params: Any) -> Any:
            if args:
                params.update(args[0])
            result = MagicMock()
            if "RETURN n" in query:
                result.single.return_value = {"n": params["props"]}
            else:
                result.single.return_value = {"matched": 0}
            result.consume.return_value.counters = Counters()
            return result
    class MissingSession(Session):
        def execute_write(self, callback: Any) -> Any:
            return callback(MissingTx())
    driver.session.side_effect = lambda **kwargs: MissingSession()
    pipeline.load = lambda: project_rais(driver, "neo4j", observation("rais"))  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="missing or duplicated"):
        pipeline.run()
    assert pipeline.load_metrics.untracked_writes == 1
    assert pipeline.load_metrics.failed_batches == 1
    assert pipeline.load_metrics.direct_updates == 0
    assert persisted_statuses == ["running", "quality_fail"]


def test_projection_retry_counts_only_summary_from_committed_attempt() -> None:
    pipeline, driver = fixture()
    class RetryingSession(Session):
        def execute_write(self, callback: Any) -> Any:
            callback(Tx())
            return callback(Tx())
    driver.session.side_effect = lambda **kwargs: RetryingSession()
    pipeline.load = lambda: project_rais(  # type: ignore[method-assign]
        driver, "neo4j", observation("rais")
    )
    pipeline.run()
    assert pipeline.load_metrics.direct_updates == 4
    assert pipeline.load_metrics.nodes_created == 2
    assert pipeline.load_metrics.relationships_created == 2


def test_loader_constructed_during_run_cannot_write_after_finalisation() -> None:
    pipeline, driver = fixture()
    started, release = Event(), Event()
    errors: list[BaseException] = []
    threads: list[Thread] = []
    def worker() -> None:
        loader = Neo4jBatchLoader(driver, metrics=pipeline.load_metrics)
        started.set()
        assert release.wait(3)
        try:
            loader.load_nodes("Late", [{"id": "late"}], "id")
        except BaseException as exc:
            errors.append(exc)
    def load() -> None:
        pipeline.load_metrics.direct_updates += 1
        thread = Thread(target=worker)
        threads.append(thread)
        thread.start()
        assert started.wait(2)
    pipeline.load = load  # type: ignore[method-assign]
    pipeline.run()
    release.set()
    for thread in threads:
        thread.join(3)
        assert not thread.is_alive()
    assert errors and isinstance(errors[0], RuntimeError)
    assert pipeline.load_metrics.verified_rows == 0


def test_inflight_loader_forces_quality_fail_at_finalisation() -> None:
    persisted_statuses.clear()
    pipeline, driver = fixture()
    started, release = Event(), Event()
    threads: list[Thread] = []
    worker_errors: list[BaseException] = []
    class BlockingTx(Tx):
        def run(self, query: str, *args: Any, **params: Any) -> Any:
            if args:
                params.update(args[0])
            started.set()
            assert release.wait(3)
            return super().run(query, **params)
    class BlockingSession(Session):
        def begin_transaction(self) -> BlockingTx:
            return BlockingTx()
    driver.session.side_effect = lambda **kwargs: BlockingSession()
    def load() -> None:
        def worker() -> None:
            try:
                Neo4jBatchLoader(driver, metrics=pipeline.load_metrics).load_nodes(
                    "Late", [{"id": "late"}], "id"
                )
            except BaseException as exc:
                worker_errors.append(exc)
        thread = Thread(target=worker)
        threads.append(thread)
        thread.start()
        assert started.wait(2)
    pipeline.load = load  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="active workers"):
        pipeline.run()
    def public_metrics() -> dict[str, Any]:
        return {key: value for key, value in vars(pipeline.load_metrics).items()
                if not key.startswith("_")}
    snapshot = public_metrics()
    release.set()
    for thread in threads:
        thread.join(3)
        assert not thread.is_alive()
    assert persisted_statuses == ["running", "quality_fail"]
    assert pipeline.load_metrics._finalized
    assert worker_errors
    assert public_metrics() == snapshot


def test_final_metrics_are_immutable() -> None:
    metrics = LoadMetrics()
    metrics.seal()
    with pytest.raises(RuntimeError, match="finalized"):
        metrics.record_untracked_write()


def test_old_worker_cannot_attach_to_next_run_and_explicit_owner_is_sealed() -> None:
    pipeline, driver = fixture()
    unbound_release = Event()
    bound_release = Event()
    run_two_started = Event()
    thread_errors: dict[str, list[BaseException]] = {"unbound": [], "bound": []}
    run_one_metrics: list[LoadMetrics] = []
    threads: list[Thread] = []
    runs = 0

    def delayed_worker(kind: str, release: Event, metrics: LoadMetrics | None = None) -> None:
        assert release.wait(3)
        try:
            loader = Neo4jBatchLoader(driver, metrics=metrics)
            loader.load_nodes("Delayed", [{"id": kind}], "id")
        except BaseException as exc:
            thread_errors[kind].append(exc)

    def load() -> None:
        nonlocal runs
        runs += 1
        if runs == 1:
            captured = pipeline.load_metrics
            run_one_metrics.append(captured)
            for kind, release, metrics in (
                ("unbound", unbound_release, None),
                ("bound", bound_release, captured),
            ):
                thread = Thread(target=delayed_worker, args=(kind, release, metrics))
                threads.append(thread)
                thread.start()
            pipeline.load_metrics.direct_updates += 1
            return

        run_two_started.set()
        unbound_release.set()
        bound_release.set()
        for thread in threads:
            thread.join(3)
            assert not thread.is_alive()
        # This synchronous worker belongs to run 2 through the active context.
        assert Neo4jBatchLoader(driver).load_nodes("Current", [{"id": "current"}], "id") == 1
        pipeline.load_metrics.direct_updates += 1

    pipeline.load = load  # type: ignore[method-assign]
    pipeline.run()
    # Run 2 is active and its own registration is installed before the callback
    # releases either delayed worker.
    pipeline.run()

    assert run_two_started.is_set()
    assert len(run_one_metrics) == 1
    assert run_one_metrics[0]._finalized
    assert pipeline.load_metrics.verified_rows == 1
    assert len(thread_errors["unbound"]) == 1
    assert isinstance(thread_errors["unbound"][0], RuntimeError)
    assert len(thread_errors["bound"]) == 1
    assert isinstance(thread_errors["bound"][0], RuntimeError)

    # Legitimate standalone loader use remains independent of pipeline metrics.
    standalone = Neo4jBatchLoader(driver)
    assert standalone.load_nodes("Standalone", [{"id": "outside"}], "id") == 1
    assert standalone.metrics.verified_rows == 1
