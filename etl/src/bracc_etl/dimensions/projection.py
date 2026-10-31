"""Project explicitly supplied dimensions and RAIS references in one transaction."""

from typing import Any

from neo4j import Driver, ManagedTransaction

from bracc_etl.loader import LoadMetrics, active_metrics_for_driver

from .materialize import materialize

_KEYS = {
    "Territory": "territory_id",
    "PublicBody": "public_body_id",
    "EconomicActivity": "activity_id",
    "Unit": "unit_id",
    "FiscalYear": "fiscal_year_id",
}
_RELATIONS = {
    "territory": ("Territory", "territory_id", "IN_TERRITORY"),
    "public_body": ("PublicBody", "public_body_id", "REFERENCES_BODY"),
    "activity": ("EconomicActivity", "activity_id", "IN_ACTIVITY"),
    "unit": ("Unit", "unit_id", "MEASURED_IN"),
    "fiscal_year": ("FiscalYear", "fiscal_year_id", "IN_FISCAL_YEAR"),
}


def project_rais(
    driver: Driver,
    database: str | None,
    observations: list[dict[str, Any]],
    *,
    metrics: LoadMetrics | None = None,
) -> None:
    """Create dimensions and links to existing LaborStats in a transaction.

    Call only after LaborStats nodes exist; no implicit identifiers or entities.
    For dispatched workers, capture run metrics before dispatch and pass ``metrics=``.
    Schema constraints must be installed separately to protect concurrent writers.
    """
    _project(driver, database, observations, "LaborStats", "stats_id", metrics)


def project_siconfi(
    driver: Driver,
    database: str | None,
    observations: list[dict[str, Any]],
    *,
    metrics: LoadMetrics | None = None,
) -> None:
    """Project dimensions onto existing MunicipalFinance facts.

    Dispatched workers must capture run metrics before dispatch and pass ``metrics=``.
    """
    _project(driver, database, observations, "MunicipalFinance", "finance_id", metrics)


def _project(
    driver: Driver,
    database: str | None,
    observations: list[dict[str, Any]],
    fact_label: str,
    fact_key: str,
    metrics: LoadMetrics | None = None,
) -> None:
    metrics = metrics if metrics is not None else active_metrics_for_driver(driver)
    result = materialize([item["dimensions"] for item in observations])
    operation = metrics.operation() if metrics is not None else _NullOperation()
    def write(tx: ManagedTransaction) -> dict[str, int]:
        counters = {name: 0 for name in (
            "nodes_created", "relationships_created", "nodes_deleted",
            "relationships_deleted", "properties_set", "labels_added", "labels_removed",
        )}

        def consume(query: str, **params: Any) -> Any:
            return tx.run(query, **params)

        def account(record: Any) -> None:
            summary = record.consume()
            for name in counters:
                counters[name] += getattr(summary.counters, name)

        for record in result["dimensions"]:
            props = dict(record)
            label = props.pop("kind")
            key = _KEYS[label]
            query = f"MERGE (n:{label} {{{key}: $id}}) ON CREATE SET n += $props RETURN n"
            response = consume(query, id=props[key], props=props)
            existing = response.single(strict=True)["n"]
            account(response)
            if any(existing.get(k) != v for k, v in props.items()):
                raise ValueError(f"Conflicting existing {label} dimension: {props[key]}")
        for item, refs in zip(observations, result["references"], strict=True):
            for name, identity in refs.items():
                label, key, relation = _RELATIONS[name]
                query = (
                    f"MATCH (s:{fact_label} {{{fact_key}: $fact_id}}) "
                    f"MATCH (d:{label} {{{key}: $id}}) "
                    f"MERGE (s)-[:{relation}]->(d) RETURN count(s) AS matched"
                )
                response = consume(query, fact_id=item[fact_key], id=identity)
                matched = response.single(strict=True)["matched"]
                account(response)
                if matched != 1:
                    raise ValueError(f"{fact_label} missing or duplicated")
            if fact_label == "MunicipalFinance":
                complete = "public_body" in refs
                response = consume(
                    "MATCH (s:MunicipalFinance {finance_id: $fact_id}) "
                    "SET s.dimension_status = $status, s.dimension_reason = $reason",
                    fact_id=item[fact_key],
                    status="projected" if complete else "partial",
                    reason="" if complete else "missing_public_body_metadata",
                )
                account(response)
        return counters

    try:
        with operation:
            with driver.session(database=database) as session:
                counters = session.execute_write(write)
            if metrics is not None:
                with metrics._lock:
                    metrics.ensure_open()
                    for name, value in counters.items():
                        setattr(metrics, name, getattr(metrics, name) + value)
                    metrics.direct_updates += sum(counters.values())
                    metrics.verified_rows += len(observations)
    except Exception:
        if metrics is not None:
            with metrics._lock:
                if not metrics._finalized:
                    metrics.failed_batches += 1
                    metrics.untracked_writes += 1
        raise


class _NullOperation:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *args: Any) -> None:
        return None
