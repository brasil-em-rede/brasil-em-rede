"""Run against a disposable Neo4j by setting BRACC_TEST_NEO4J_URI."""

import os
from uuid import uuid4

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

from bracc_etl.loader import Neo4jBatchLoader


@pytest.mark.skipif(not os.getenv("BRACC_TEST_NEO4J_URI"), reason="requires disposable Neo4j")
def test_transactions_idempotence_and_actual_counters():
    driver = GraphDatabase.driver(os.environ["BRACC_TEST_NEO4J_URI"], auth=None)
    try:
        loader = Neo4jBatchLoader(driver, batch_size=2)
        prefix = uuid4().hex
        rows = [{"id": prefix + "one"}, {"id": prefix + "two"}]
        assert loader.load_nodes("F06Node", rows, "id") == 2
        assert loader.metrics.nodes_created == 2
        assert loader.load_nodes("F06Node", rows, "id") == 2
        assert loader.metrics.nodes_created == 2
        links = [
            {"source_key": prefix + "one", "target_key": prefix + "two"},
            {"source_key": prefix + "one", "target_key": prefix + "absent"},
        ]
        assert loader.load_relationships("F06_LINK", links, "F06Node", "id", "F06Node", "id") == 1
        assert loader.metrics.relationships_created == 1
        assert loader.metrics.unresolved == 1
        assert loader.load_relationships("F06_LINK", links, "F06Node", "id", "F06Node", "id") == 1
        assert loader.metrics.relationships_created == 1
        with pytest.raises(Neo4jError):
            loader.run_query(
                "UNWIND $rows AS row CREATE (:F06Failure {id: row.id, value: row.value})",
                [
                    {"id": prefix + "1", "value": 1},
                    {"id": prefix + "2", "value": 2},
                    {"id": prefix + "3", "value": {"invalid": 1}},
                ],
            )
        with driver.session() as session:
            assert (
                session.run(
                    "MATCH (n:F06Failure) WHERE n.id STARTS WITH $prefix RETURN count(n) AS n",
                    prefix=prefix,
                ).single()["n"]
                == 2
            )
        assert loader.metrics.committed_rows == 10
        assert loader.metrics.attempts == 6
    finally:
        driver.close()


@pytest.mark.skipif(not os.getenv("BRACC_TEST_NEO4J_URI"), reason="requires disposable Neo4j")
def test_pipeline_records_partial_commits_after_last_batch_failure():
    from bracc_etl.base import Pipeline

    class Synthetic(Pipeline):
        name = "cnpj"
        source_id = "receita_federal"

        def extract(self):
            pass

        def transform(self):
            pass

        def load(self):
            loader = Neo4jBatchLoader(self.driver, batch_size=2)
            loader.run_query(
                "UNWIND $rows AS row CREATE (:F06Pipeline {value: row.value})",
                [{"value": 1}, {"value": 2}, {"value": {"invalid": 1}}],
            )

    driver = GraphDatabase.driver(os.environ["BRACC_TEST_NEO4J_URI"], auth=None)
    try:
        pipeline = Synthetic(driver)
        pipeline.run_id = uuid4().hex
        with pytest.raises(Neo4jError):
            pipeline.run()
        with driver.session() as session:
            run = session.run(
                "MATCH (r:IngestionRun {run_id: $id}) RETURN r", id=pipeline.run_id
            ).single()["r"]
        assert run["status"] == "quality_fail"
        assert (run["submitted"], run["attempts"], run["committed_rows"]) == (3, 2, 2)
        assert (run["rows_loaded"], run["nodes_created"], run["rejected"]) == (0, 2, 0)
        assert run["generic_updates"] >= 2
    finally:
        driver.close()


@pytest.mark.skipif(not os.getenv("BRACC_TEST_NEO4J_URI"), reason="requires disposable Neo4j")
def test_duplicate_endpoints_abort_entire_batch():
    driver = GraphDatabase.driver(os.environ["BRACC_TEST_NEO4J_URI"], auth=None)
    prefix = uuid4().hex
    try:
        with driver.session() as session:
            session.run(
                "CREATE (:F06Duplicate {id: $a}), (:F06Duplicate {id: $a}), "
                "(:F06Duplicate {id: $b})",
                a=prefix + "a",
                b=prefix + "b",
            ).consume()
        loader = Neo4jBatchLoader(driver)
        with pytest.raises(ValueError, match="not unique"):
            loader.load_relationships(
                "F06_DUP",
                [
                    {"source_key": prefix + "a", "target_key": prefix + "b"},
                    {"source_key": prefix + "b", "target_key": prefix + "b"},
                ],
                "F06Duplicate",
                "id",
                "F06Duplicate",
                "id",
            )
        with driver.session() as session:
            assert (
                session.run(
                    "MATCH (a:F06Duplicate)-[r:F06_DUP]->() WHERE a.id STARTS WITH $prefix "
                    "RETURN count(r) AS n",
                    prefix=prefix,
                ).single()["n"]
                == 0
            )
        assert loader.metrics.committed_rows == 0
    finally:
        driver.close()


@pytest.mark.skipif(not os.getenv("BRACC_TEST_NEO4J_URI"), reason="requires disposable Neo4j")
def test_pipeline_no_match_fails_and_idempotent_nodes_succeed():
    from bracc_etl.base import Pipeline

    class Synthetic(Pipeline):
        name = "cnpj"
        source_id = "receita_federal"
        mode = "no_match"
        key = uuid4().hex

        def extract(self):
            pass

        def transform(self):
            pass

        def load(self):
            loader = Neo4jBatchLoader(self.driver)
            if self.mode == "no_match":
                loader.run_query("UNWIND $rows AS row WITH row WHERE false RETURN row", [{"id": 1}])
            else:
                loader.load_nodes("F06Idempotent", [{"id": self.key}], "id")

    driver = GraphDatabase.driver(os.environ["BRACC_TEST_NEO4J_URI"], auth=None)
    try:
        pipeline = Synthetic(driver)
        with pytest.raises(ValueError, match="empty load requires evidence"):
            pipeline.run()
        first_id = pipeline.run_id
        with driver.session() as session:
            run = session.run(
                "MATCH (r:IngestionRun {run_id: $id}) RETURN r", id=first_id
            ).single()["r"]
        assert (
            run["status"], run["rows_loaded"], run["generic_updates"],
            run["submitted"], run["committed_rows"], run["verified_rows"],
            run["nodes_created"], run["relationships_created"],
        ) == ("quality_fail", 0, 0, 1, 1, 0, 0, 0)
        pipeline.mode = "nodes"
        pipeline.run()
        second_id = pipeline.run_id
        pipeline.run()
        assert len({first_id, second_id, pipeline.run_id}) == 3
        with driver.session() as session:
            run = session.run(
                "MATCH (r:IngestionRun {run_id: $id}) RETURN r", id=pipeline.run_id
            ).single()["r"]
        assert (run["status"], run["rows_loaded"], run["nodes_created"]) == ("loaded", 1, 0)
    finally:
        driver.close()


@pytest.mark.skipif(not os.getenv("BRACC_TEST_NEO4J_URI"), reason="requires disposable Neo4j")
def test_threaded_loader_metrics_and_direct_commit_receipt():
    from concurrent.futures import ThreadPoolExecutor

    from bracc_etl.base import Pipeline

    class Synthetic(Pipeline):
        name = "cnpj"
        source_id = "receita_federal"

        def extract(self):
            pass

        def transform(self):
            pass

        def load(self):
            prefix = uuid4().hex
            metrics = self.load_metrics

            def worker(i):
                loader = Neo4jBatchLoader(self.driver, metrics=metrics)
                loader.load_nodes("F06Thread", [{"id": f"{prefix}-{i}"}], "id")

            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(worker, range(4)))
            self.run_direct_write("CREATE (:F06Direct {id: $id})", {"id": prefix})

    driver = GraphDatabase.driver(os.environ["BRACC_TEST_NEO4J_URI"], auth=None)
    try:
        pipeline = Synthetic(driver)
        pipeline.run()
        with driver.session() as session:
            run = session.run(
                "MATCH (r:IngestionRun {run_id: $id}) RETURN r", id=pipeline.run_id
            ).single()["r"]
        assert (run["nodes_created"], run["rows_loaded"], run["direct_updates"]) == (5, 4, 3)
    finally:
        driver.close()
