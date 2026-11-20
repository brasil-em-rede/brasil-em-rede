from __future__ import annotations

import os
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
from neo4j import GraphDatabase

from bracc_etl.dimensions.projection import project_rais, project_siconfi
from bracc_etl.loader import LoadMetrics
from bracc_etl.pipelines.rais import RaisPipeline
from bracc_etl.pipelines.siconfi import SiconfiPipeline

pytestmark = pytest.mark.integration

CONFIRMED_WRITE_FIELDS = (
    "nodes_created",
    "relationships_created",
    "nodes_deleted",
    "relationships_deleted",
    "properties_set",
    "labels_added",
    "labels_removed",
    "direct_updates",
    "verified_rows",
)


def confirmed_write_metrics(metrics: LoadMetrics) -> dict[str, int]:
    return {field: getattr(metrics, field) for field in CONFIRMED_WRITE_FIELDS}


def assert_confirmed_metrics(metrics: LoadMetrics, expected: dict[str, int]) -> None:
    assert confirmed_write_metrics(metrics) == expected


RAIS_ROWS = [
    {
        "cnae_subclass": "0111301",
        "uf": "SP",
        "ibge_code": "35",
        "territorial_version": "2022",
        "cnae_version": "2.0",
        "unit_system": "RAIS",
        "unit_code": "vinculos",
        "exercise": "2022",
        "establishment_count": "4",
        "total_employees": "9",
    },
    {
        "cnae_subclass": "0111301",
        "uf": "RJ",
        "ibge_code": "33",
        "territorial_version": "2022",
        "cnae_version": "2.0",
        "unit_system": "RAIS",
        "unit_code": "estabelecimentos",
        "exercise": "2022",
        "establishment_count": "2",
        "total_employees": "3",
    },
]


def siconfi_row(account: str, *, body: bool = True) -> dict[str, str]:
    row = {
        "cod_ibge": "3550308",
        "exercicio": "2023",
        "uf": "SP",
        "versao_territorial": "dtb-test-v1",
        "tipo_territorio": "municipio",
        "unidade_sistema": "SICONFI",
        "unidade_codigo": "BRL",
        "conta": account,
        "coluna": "valor",
        "valor": "100",
        "instituicao": "Synthetic reporting institution",
    }
    if body:
        row.update(
            {
                "orgao_jurisdicao": "SP",
                "orgao_sistema": "synthetic",
                "orgao_papel": "ug",
                "orgao_codigo": "000123",
                "orgao_versao": "fixture-v1",
            }
        )
    return row


def rais_fixture() -> tuple[RaisPipeline, list[dict[str, Any]]]:
    pipeline = RaisPipeline(MagicMock())
    pipeline._from_aggregated(pd.DataFrame(RAIS_ROWS))
    observations = [
        {"stats_id": item["stats_id"], "dimensions": item["dimensions"]}
        for item in pipeline.dimension_observations
    ]
    return pipeline, observations


def siconfi_fixture(
    rows: list[dict[str, str]],
) -> tuple[SiconfiPipeline, list[dict[str, Any]]]:
    pipeline = SiconfiPipeline(MagicMock())
    pipeline._raw = rows
    pipeline.transform()
    observations = [
        {"finance_id": item["finance_id"], "dimensions": item["dimensions"]}
        for item in pipeline.dimension_observations
    ]
    return pipeline, observations


def node_snapshot(driver: Any) -> list[tuple[str, str, dict[str, Any]]]:
    identity_fields = {
        "LaborStats": "stats_id",
        "MunicipalFinance": "finance_id",
        "Territory": "territory_id",
        "PublicBody": "public_body_id",
        "EconomicActivity": "activity_id",
        "Unit": "unit_id",
        "FiscalYear": "fiscal_year_id",
    }
    with driver.session(database="neo4j") as session:
        records = session.run(
            "MATCH (n) RETURN labels(n) AS labels, properties(n) AS properties"
        ).data()
    result = [
        (
            record["labels"][0],
            record["properties"][identity_fields[record["labels"][0]]],
            record["properties"],
        )
        for record in records
    ]
    return sorted(result, key=lambda item: (item[0], item[1]))


def relation_snapshot(driver: Any) -> list[tuple[str, str, str, str, str]]:
    identity_fields = {
        "LaborStats": "stats_id",
        "MunicipalFinance": "finance_id",
        "Territory": "territory_id",
        "PublicBody": "public_body_id",
        "EconomicActivity": "activity_id",
        "Unit": "unit_id",
        "FiscalYear": "fiscal_year_id",
    }
    with driver.session(database="neo4j") as session:
        records = session.run(
            "MATCH (s)-[r]->(d) RETURN labels(s) AS source_labels, "
            "properties(s) AS source, type(r) AS relation, "
            "labels(d) AS target_labels, properties(d) AS target"
        ).data()
    return sorted(
        (
            record["source_labels"][0],
            record["source"][identity_fields[record["source_labels"][0]]],
            record["relation"],
            record["target_labels"][0],
            record["target"][identity_fields[record["target_labels"][0]]],
        )
        for record in records
    )


def graph_counts(driver: Any) -> tuple[int, int]:
    with driver.session(database="neo4j") as session:
        record = session.run(
            "MATCH (n) WITH count(n) AS nodes "
            "OPTIONAL MATCH ()-[r]->() RETURN nodes, count(r) AS relationships"
        ).single(strict=True)
    return record["nodes"], record["relationships"]


def create_constraints(driver: Any) -> None:
    constraints = (
        "CREATE CONSTRAINT f09_laborstats_id IF NOT EXISTS "
        "FOR (n:LaborStats) REQUIRE n.stats_id IS UNIQUE",
        "CREATE CONSTRAINT f09_finance_id IF NOT EXISTS "
        "FOR (n:MunicipalFinance) REQUIRE n.finance_id IS UNIQUE",
        "CREATE CONSTRAINT f09_territory_id IF NOT EXISTS "
        "FOR (n:Territory) REQUIRE n.territory_id IS UNIQUE",
        "CREATE CONSTRAINT f09_body_id IF NOT EXISTS "
        "FOR (n:PublicBody) REQUIRE n.public_body_id IS UNIQUE",
        "CREATE CONSTRAINT f09_activity_id IF NOT EXISTS "
        "FOR (n:EconomicActivity) REQUIRE n.activity_id IS UNIQUE",
        "CREATE CONSTRAINT f09_unit_id IF NOT EXISTS "
        "FOR (n:Unit) REQUIRE n.unit_id IS UNIQUE",
        "CREATE CONSTRAINT f09_year_id IF NOT EXISTS "
        "FOR (n:FiscalYear) REQUIRE n.fiscal_year_id IS UNIQUE",
    )
    with driver.session(database="neo4j") as session:
        for statement in constraints:
            session.run(statement).consume()


def seed_facts(driver: Any, rais: list[dict[str, Any]], siconfi: list[dict[str, Any]]) -> None:
    with driver.session(database="neo4j") as session:
        for item in rais:
            row = next(
                row
                for row in RAIS_ROWS
                if f"rais_2022_{row['cnae_subclass']}_{row['uf']}" == item["stats_id"]
            )
            session.run(
                "CREATE (:LaborStats {stats_id: $id, cnae_subclass: $cnae, uf: $uf, year: 2022})",
                id=item["stats_id"],
                cnae=row["cnae_subclass"],
                uf=row["uf"],
            ).consume()
        for item in siconfi:
            session.run(
                "CREATE (:MunicipalFinance {finance_id: $id, cod_ibge: '3550308', "
                "year: '2023', institution_name: 'Synthetic reporting institution'})",
                id=item["finance_id"],
            ).consume()


@pytest.fixture
def driver() -> Any:
    uri = os.environ["BRACC_TEST_NEO4J_URI"]
    password = os.environ["BRACC_TEST_NEO4J_PASSWORD"]
    database = os.environ.get("BRACC_TEST_NEO4J_DATABASE", "neo4j")
    instance = GraphDatabase.driver(uri, auth=("neo4j", password))
    try:
        instance.verify_connectivity()
        with instance.session(database=database) as session:
            assert session.run("RETURN 1 AS ready").single(strict=True)["ready"] == 1
            session.run("MATCH (n) DETACH DELETE n").consume()
        create_constraints(instance)
        yield instance
    finally:
        instance.close()


def test_projectors_confirm_independent_facts_dimensions_and_metrics(driver: Any) -> None:
    rais_pipeline, rais_observations = rais_fixture()
    siconfi_pipeline, siconfi_observations = siconfi_fixture(
        [siconfi_row("receita"), siconfi_row("despesa", body=False)]
    )
    metrics = LoadMetrics()
    seed_facts(driver, rais_observations, siconfi_observations)
    before_nodes, before_relationships = graph_counts(driver)
    before_status = {
        item["finance_id"]: item["dimension_reason"] for item in siconfi_pipeline.finances
    }

    for _ in range(2):
        project_rais(driver, "neo4j", rais_observations, metrics=metrics)
        project_siconfi(driver, "neo4j", siconfi_observations, metrics=metrics)

    nodes = node_snapshot(driver)
    links = relation_snapshot(driver)
    after_nodes, after_relationships = graph_counts(driver)
    with driver.session(database="neo4j") as session:
        persisted_facts = session.run(
            "MATCH (n:MunicipalFinance) RETURN n.finance_id AS finance_id, "
            "n.dimension_status AS dimension_status, n.dimension_reason AS dimension_reason"
        ).data()
    created_nodes = after_nodes - before_nodes
    created_relationships = after_relationships - before_relationships
    assert created_nodes == 10
    assert created_relationships == 15
    assert len(nodes) == 14
    assert len(links) == 15
    assert len({(label, identity) for label, identity, _ in nodes}) == len(nodes)
    body_fact = next(
        item for item in siconfi_observations if "public_body" in item["dimensions"]
    )
    partial_fact = next(
        item for item in siconfi_observations if "public_body" not in item["dimensions"]
    )
    expected_status = {
        body_fact["finance_id"]: ("projected", ""),
        partial_fact["finance_id"]: ("partial", "missing_public_body_metadata"),
    }
    assert {
        row["finance_id"]: (row["dimension_status"], row["dimension_reason"])
        for row in persisted_facts
    } == expected_status
    assert {
        row["finance_id"]: row["dimension_reason"] for row in siconfi_pipeline.finances
    } == before_status
    assert not any(label in {"Person", "Company"} for label, _, _ in nodes)

    expected_nodes: dict[tuple[str, str], dict[str, Any]] = {
        ("LaborStats", "rais_2022_0111301_SP"): {
            "stats_id": "rais_2022_0111301_SP", "cnae_subclass": "0111301", "uf": "SP", "year": 2022
        },
        ("LaborStats", "rais_2022_0111301_RJ"): {
            "stats_id": "rais_2022_0111301_RJ", "cnae_subclass": "0111301", "uf": "RJ", "year": 2022
        },
        ("Territory", "IBGE:uf:35:2022"): {
            "territory_id": "IBGE:uf:35:2022", "system": "IBGE", "type": "uf",
            "code": "35", "territorial_version": "2022", "uf": "SP"
        },
        ("Territory", "IBGE:uf:33:2022"): {
            "territory_id": "IBGE:uf:33:2022", "system": "IBGE", "type": "uf",
            "code": "33", "territorial_version": "2022", "uf": "RJ"
        },
        ("Territory", "IBGE:municipio:3550308:dtb-test-v1"): {
            "territory_id": "IBGE:municipio:3550308:dtb-test-v1", "system": "IBGE",
            "type": "municipio", "code": "3550308", "territorial_version": "dtb-test-v1",
            "uf": "SP"
        },
        ("EconomicActivity", "CNAE:2.0:0111301"): {
            "activity_id": "CNAE:2.0:0111301", "classification": "CNAE",
            "version": "2.0", "code": "0111301"
        },
        ("Unit", "RAIS:vinculos"): {
            "unit_id": "RAIS:vinculos", "system": "RAIS", "code": "vinculos"
        },
        ("Unit", "RAIS:estabelecimentos"): {
            "unit_id": "RAIS:estabelecimentos", "system": "RAIS",
            "code": "estabelecimentos"
        },
        ("Unit", "SICONFI:BRL"): {
            "unit_id": "SICONFI:BRL", "system": "SICONFI", "code": "BRL"
        },
        ("FiscalYear", "2022"): {"fiscal_year_id": "2022", "year": "2022"},
        ("FiscalYear", "2023"): {"fiscal_year_id": "2023", "year": "2023"},
        ("PublicBody", "SP:synthetic:ug:000123:fixture-v1"): {
            "public_body_id": "SP:synthetic:ug:000123:fixture-v1", "jurisdiction": "SP",
            "system": "synthetic", "role": "ug", "code": "000123", "version": "fixture-v1"
        },
    }
    for item in siconfi_observations:
        with_body = "public_body" in item["dimensions"]
        expected_nodes[("MunicipalFinance", item["finance_id"])] = {
            "finance_id": item["finance_id"], "cod_ibge": "3550308", "year": "2023",
            "institution_name": "Synthetic reporting institution",
            "dimension_status": "projected" if with_body else "partial",
            "dimension_reason": "" if with_body else "missing_public_body_metadata"
        }
    actual_nodes = {(label, identity): properties for label, identity, properties in nodes}
    assert actual_nodes == expected_nodes
    expected_dimension_nodes = {
        key: properties
        for key, properties in expected_nodes.items()
        if key[0]
        in {"Territory", "PublicBody", "EconomicActivity", "Unit", "FiscalYear"}
    }

    rais_rows_by_id = {
        f"rais_2022_{row['cnae_subclass']}_{row['uf']}": row for row in RAIS_ROWS
    }
    rais_expected = []
    for item in rais_observations:
        row = rais_rows_by_id[item["stats_id"]]
        territory_id = "IBGE:uf:35:2022" if row["uf"] == "SP" else "IBGE:uf:33:2022"
        rais_expected.extend(
            [
                ("LaborStats", item["stats_id"], "IN_TERRITORY", "Territory", territory_id),
                (
                    "LaborStats", item["stats_id"], "IN_ACTIVITY", "EconomicActivity",
                    "CNAE:2.0:0111301"
                ),
                (
                    "LaborStats", item["stats_id"], "MEASURED_IN", "Unit",
                    "RAIS:" + row["unit_code"]
                ),
                ("LaborStats", item["stats_id"], "IN_FISCAL_YEAR", "FiscalYear", "2022"),
            ]
        )
    siconfi_expected = []
    for item in siconfi_observations:
        siconfi_expected.extend(
            [
                (
                    "MunicipalFinance", item["finance_id"], "IN_TERRITORY", "Territory",
                    "IBGE:municipio:3550308:dtb-test-v1"
                ),
                ("MunicipalFinance", item["finance_id"], "MEASURED_IN", "Unit", "SICONFI:BRL"),
                (
                    "MunicipalFinance", item["finance_id"], "IN_FISCAL_YEAR", "FiscalYear", "2023"
                ),
            ]
        )
        if "public_body" in item["dimensions"]:
            siconfi_expected.append(
                (
                    "MunicipalFinance", item["finance_id"], "REFERENCES_BODY", "PublicBody",
                    "SP:synthetic:ug:000123:fixture-v1"
                )
            )
    assert set(links) == set(rais_expected + siconfi_expected)
    expected_created_nodes = len(expected_dimension_nodes)
    expected_created_relationships = len(rais_expected) + len(siconfi_expected)
    # MERGE sets its identity property and ON CREATE SET adds every expected property.
    # Each Siconfi fact is status-SET twice (two projector calls), with two status fields.
    expected_properties_set = sum(
        len(properties) + 1 for properties in expected_dimension_nodes.values()
    ) + len(siconfi_observations) * 2 * 2
    expected_labels_added = expected_created_nodes
    expected_confirmed = {
        "nodes_created": expected_created_nodes,
        "relationships_created": expected_created_relationships,
        "nodes_deleted": 0,
        "relationships_deleted": 0,
        "properties_set": expected_properties_set,
        "labels_added": expected_labels_added,
        "labels_removed": 0,
    }
    expected_confirmed["direct_updates"] = sum(expected_confirmed.values())
    expected_confirmed["verified_rows"] = 2 * (
        len(rais_observations) + len(siconfi_observations)
    )
    assert created_nodes == expected_confirmed["nodes_created"]
    assert created_relationships == expected_confirmed["relationships_created"]
    assert_confirmed_metrics(metrics, expected_confirmed)
    assert {
        props["dimension_status"]
        for label, _, props in nodes
        if label == "MunicipalFinance"
    } == {"projected", "partial"}
    assert rais_pipeline.labor_stats[0]["stats_id"] == rais_observations[0]["stats_id"]
    assert len(siconfi_pipeline.finances) == len(siconfi_observations)


def test_existing_dimension_conflict_rolls_back_and_preserves_metrics(driver: Any) -> None:
    _, rais_observations = rais_fixture()
    metrics = LoadMetrics()
    seed_facts(driver, rais_observations[:1], [])
    with driver.session(database="neo4j") as session:
        session.run(
            "CREATE (:Territory {territory_id: 'IBGE:uf:35:2022', system: 'IBGE', "
            "type: 'uf', code: '35', territorial_version: '2022', uf: 'RJ'})"
        ).consume()
    before_graph = (node_snapshot(driver), relation_snapshot(driver))
    before_confirmed = confirmed_write_metrics(metrics)
    before_failed_batches = metrics.failed_batches
    before_untracked_writes = metrics.untracked_writes
    with pytest.raises(
        ValueError, match=r"^Conflicting existing Territory dimension: IBGE:uf:35:2022$"
    ):
        project_rais(driver, "neo4j", rais_observations[:1], metrics=metrics)
    assert (node_snapshot(driver), relation_snapshot(driver)) == before_graph
    assert_confirmed_metrics(metrics, before_confirmed)
    assert metrics.failed_batches == before_failed_batches + 1
    assert metrics.untracked_writes == before_untracked_writes + 1


def test_missing_fact_rolls_back_prior_dimension_merges(driver: Any) -> None:
    _, rais_observations = rais_fixture()
    metrics = LoadMetrics()
    seed_facts(driver, rais_observations[:1], [])
    before_graph = (node_snapshot(driver), relation_snapshot(driver))
    before_confirmed = confirmed_write_metrics(metrics)
    before_failed_batches = metrics.failed_batches
    before_untracked_writes = metrics.untracked_writes
    invalid = [{**rais_observations[0], "stats_id": "absent-laborstats"}]
    with pytest.raises(ValueError, match="^LaborStats missing or duplicated$"):
        project_rais(driver, "neo4j", invalid, metrics=metrics)
    assert (node_snapshot(driver), relation_snapshot(driver)) == before_graph
    assert_confirmed_metrics(metrics, before_confirmed)
    assert metrics.failed_batches == before_failed_batches + 1
    assert metrics.untracked_writes == before_untracked_writes + 1


def test_missing_siconfi_metadata_stays_pending_without_projection(driver: Any) -> None:
    pipeline, observations = siconfi_fixture([siconfi_row("receita")])
    pending_row = siconfi_row("despesa")
    del pending_row["versao_territorial"]
    pending, pending_observations = siconfi_fixture([pending_row])
    assert len(observations) == 1
    assert pending_observations == []
    assert pending.finances[0]["dimension_reason"] == "missing_metadata:versao_territorial"
    assert pipeline.finances[0]["dimension_status"] == "pending"
    assert pending.finances[0]["dimension_status"] == "pending"

    metrics = LoadMetrics()
    before_confirmed = confirmed_write_metrics(metrics)
    before = (node_snapshot(driver), relation_snapshot(driver))
    project_siconfi(driver, "neo4j", pending_observations, metrics=metrics)
    assert (node_snapshot(driver), relation_snapshot(driver)) == before
    assert_confirmed_metrics(metrics, before_confirmed)
