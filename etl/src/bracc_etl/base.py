import csv
import hashlib
import json
import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from neo4j import Driver

from bracc_etl.extraction import (
    ExtractionContractError,
    ExtractionResult,
    TransformPlan,
    execute_transform,
)
from bracc_etl.loader import (
    LoadMetrics,
    _active_load,
    register_active_metrics,
    unregister_active_metrics,
)
from bracc_etl.source_aliases import SCHEMA_VERSION, source_aliases

logger = logging.getLogger(__name__)


def _scoped_pipeline_write(method: Any) -> Any:
    @wraps(method)
    def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        with self.load_metrics.operation():
            return method(self, *args, **kwargs)

    return wrapped


@dataclass(frozen=True)
class EmptyLoadEvidence:
    """An acquisition manifest and its actual JSON or CSV source artifact."""

    path: str
    sha256: str
    artifact_path: str = ""
    source_url: str = ""
    artifact_format: str = ""

    def verify(self, source_id: str, configured_source_url: str | None = None) -> bool:
        try:
            content = Path(self.path).read_bytes()
            artifact = Path(self.artifact_path)
            raw = artifact.read_bytes()
            manifest = json.loads(content)
            pages = manifest["pages"]
            if not isinstance(pages, dict) or len(pages) != 1:
                return False
            record = pages[artifact.name]
            url = urlsplit(self.source_url)
            if not (
                hashlib.sha256(content).hexdigest() == self.sha256
                and manifest["source_id"] == source_id
                and self.source_url == configured_source_url
                and url.scheme in {"http", "https", "ftp"}
                and bool(url.hostname)
                and not url.username
                and not url.password
                and record["source_sha256"] == hashlib.sha256(self.source_url.encode()).hexdigest()
                and record["sha256"] == hashlib.sha256(raw).hexdigest()
                and record["bytes"] == len(raw)
            ):
                return False
            if self.artifact_format == "json":
                data = json.loads(raw)
                return isinstance(data, list) and len(data) == 0
            if self.artifact_format == "csv":
                import io

                reader = csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""), strict=True)
                header = next(reader, None)
                return bool(header) and next(reader, None) is None
            return False
        except (OSError, ValueError, TypeError, KeyError, UnicodeDecodeError, csv.Error):
            return False


class Pipeline(ABC):
    """Base class for all ETL pipelines."""

    name: str
    source_id: str

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        neo4j_database: str | None = None,
        history: bool = False,
    ) -> None:
        self.driver = driver
        self.data_dir = data_dir
        self.limit = limit
        self.chunk_size = chunk_size
        self.neo4j_database = neo4j_database or os.getenv("NEO4J_DATABASE", "neo4j")
        self.history = history
        self.rows_in: int = 0
        self.rows_loaded: int = 0
        self.empty_load_evidence: EmptyLoadEvidence | None = None
        self.configured_source_url: str | None = None
        self.extraction_adapter: Any | None = None
        self.load_metrics = LoadMetrics()
        self.source_identity = source_aliases().for_pipeline(self.name, self.source_id)
        source_key = getattr(self, "source_id", getattr(self, "name", "unknown_source"))
        self.run_id = f"{source_key}_{uuid4().hex}"

    @abstractmethod
    def extract(self) -> None:
        """Download raw data from source."""

    @abstractmethod
    def transform(self) -> None:
        """Normalize, deduplicate, and prepare data for loading."""

    @abstractmethod
    def load(self) -> None:
        """Load transformed data into Neo4j."""

    def produce_extraction(self) -> tuple[ExtractionResult, tuple[Any, ...]]:
        """Override to opt into the structured extraction contract."""
        raise ExtractionContractError("structured extraction is not configured")

    def transform_result(self, result: ExtractionResult) -> TransformPlan:
        """Transform a verified stream into the exact rows passed to structured loading."""
        raise ExtractionContractError("structured consumer is not configured")

    def load_extraction(self, records: Sequence[dict[str, Any]]) -> None:
        """Load the concrete output of transform_result for an opt-in adapter."""
        raise ExtractionContractError("structured loader is not configured")

    def run(self) -> None:
        """Execute the full ETL pipeline."""
        self.run_id = f"{self.source_id}_{uuid4().hex}"
        started_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.load_metrics = LoadMetrics()
        self.rows_in = 0
        self.rows_loaded = 0
        self._upsert_ingestion_run(status="running", started_at=started_at)
        token = _active_load.set((self.driver, self.load_metrics))
        metrics_registered = False
        cursors: tuple[Any, ...] = ()
        structured = type(self).produce_extraction is not Pipeline.produce_extraction
        try:
            register_active_metrics(self.driver, self.load_metrics)
            metrics_registered = True
            if structured:
                result, cursors = self.produce_extraction()
                if (
                    not isinstance(result, ExtractionResult)
                    or result.run_id != self.run_id
                    or result.source_id != self.source_identity.source_id
                    or result.source_url != self.configured_source_url
                ):
                    raise ExtractionContractError("producer result identity mismatch")
                owned_cursors = tuple(
                    cursor for population in result.populations for cursor in population._cursors
                )
                if owned_cursors != cursors or len({id(cursor) for cursor in cursors}) != len(
                    cursors
                ):
                    raise ExtractionContractError("cursor binding mismatch or sharing")
                original_bindings = tuple(
                    (cursor, cursor.binding)
                    for population in result.populations
                    for cursor in population._cursors
                )
                if tuple(
                    binding for population in result.populations for binding in population.bindings
                ) != tuple(binding for _, binding in original_bindings):
                    raise ExtractionContractError("manifest binding mismatch")
                plan = self.transform_result(result)
                if any(cursor.binding != binding for cursor, binding in original_bindings):
                    raise ExtractionContractError("artifact binding was mutated")
                verified_output = execute_transform(result, cursors, plan)
                current_adapter = self.extraction_adapter
                if (
                    current_adapter is None
                    or tuple(
                        (spec.population_id, spec.required) for spec in current_adapter.populations
                    )
                    != result.adapter_policy
                ):
                    raise ExtractionContractError("adapter policy binding mismatch")
                self.load_extraction(verified_output.rows)
            else:
                self.extract()
                self.transform()
                self.load()
            self.load_metrics.seal()
            self._sync_load_metrics()
            if self.load_metrics.failed_batches or self.load_metrics.untracked_writes:
                raise ValueError("load has failed batches or untracked writes")
            if (
                not structured
                and self.load_metrics.submitted == 0
                and self.rows_loaded == 0
                and self.load_metrics.generic_updates == 0
                and self.load_metrics.direct_updates == 0
                and isinstance(self.empty_load_evidence, EmptyLoadEvidence)
            ):
                raise ExtractionContractError(
                    "legacy empty evidence cannot prove extracted population"
                )
            if (
                self.rows_loaded == 0
                and self.load_metrics.generic_updates == 0
                and self.load_metrics.direct_updates == 0
                and not structured
            ):
                raise ValueError("empty load requires structured extraction evidence")
            finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._upsert_ingestion_run(
                status="loaded", started_at=started_at, finished_at=finished_at
            )
            logger.info("[%s] Pipeline complete.", self.name)
        except Exception as exc:
            with suppress(RuntimeError):
                self.load_metrics.seal()
            self._sync_load_metrics()
            finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._upsert_ingestion_run(
                status="quality_fail",
                started_at=started_at,
                finished_at=finished_at,
                error=str(exc)[:1000],
            )
            raise
        finally:
            for cursor in cursors:
                with suppress(Exception):
                    cursor.close()
            if metrics_registered:
                unregister_active_metrics(self.driver, self.load_metrics)
            _active_load.reset(token)
            self.empty_load_evidence = None

    @_scoped_pipeline_write
    def run_direct_write(self, query: str, parameters: dict[str, Any] | None = None) -> None:
        """Execute an auto-commit write and record its consumed Neo4j summary."""
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                summary = session.run(query, parameters or {}).consume()
            counters = summary.counters
            with self.load_metrics._lock:
                self.load_metrics.ensure_open()
                self.load_metrics.nodes_created += counters.nodes_created
                self.load_metrics.relationships_created += counters.relationships_created
                self.load_metrics.nodes_deleted += counters.nodes_deleted
                self.load_metrics.relationships_deleted += counters.relationships_deleted
                self.load_metrics.properties_set += counters.properties_set
                self.load_metrics.labels_added += counters.labels_added
                self.load_metrics.labels_removed += counters.labels_removed
                self.load_metrics.direct_updates += (
                    counters.nodes_created
                    + counters.relationships_created
                    + counters.nodes_deleted
                    + counters.relationships_deleted
                    + counters.properties_set
                    + counters.labels_added
                    + counters.labels_removed
                )
        except Exception:
            self.mark_untracked_write()
            raise

    def mark_untracked_write(self) -> None:
        """Fail closed when a direct write has no committed summary."""
        self.load_metrics.record_untracked_write()

    def _sync_load_metrics(self) -> None:
        self.rows_loaded = self.load_metrics.verified_rows

    def _upsert_ingestion_run(
        self,
        *,
        status: str,
        started_at: str | None = None,
        finished_at: str | None = None,
        error: str | None = None,
    ) -> None:
        """Persist ingestion run state for operational traceability."""
        identity = self.source_identity
        query = (
            "MERGE (r:IngestionRun {run_id: $run_id}) "
            "SET r.source_id = $source_id, "
            "    r.runner_name = $runner_name, "
            "    r.provider_id = $provider_id, "
            "    r.source_aliases_version = $source_aliases_version, "
            "    r.status = $status, "
            "    r.started_at = coalesce($started_at, r.started_at), "
            "    r.finished_at = coalesce($finished_at, r.finished_at), "
            "    r.error = coalesce($error, r.error), "
            "    r.rows_in = $rows_in, "
            "    r.rows_loaded = $rows_loaded, "
            "    r.empty_load_evidence_path = $empty_load_evidence_path, "
            "    r.empty_load_evidence_sha256 = $empty_load_evidence_sha256, "
            "    r.empty_load_artifact_path = $empty_load_artifact_path, "
            "    r.empty_load_source_sha256 = $empty_load_source_sha256, "
            "    r.failed_batches = $failed_batches, r.direct_updates = $direct_updates, "
            "    r.untracked_writes = $untracked_writes, "
            "    r.submitted = $submitted, r.attempts = $attempts, "
            "    r.rejected = $rejected, r.committed_rows = $committed_rows, "
            "    r.verified_rows = $verified_rows, r.generic_updates = $generic_updates, "
            "    r.nodes_created = $nodes_created, r.nodes_deleted = $nodes_deleted, "
            "    r.relationships_created = $relationships_created, "
            "    r.relationships_deleted = $relationships_deleted, "
            "    r.properties_set = $properties_set, r.labels_added = $labels_added, "
            "    r.labels_removed = $labels_removed, "
            "    r.relationships_resolved = $relationships_resolved, r.unresolved = $unresolved"
        )
        params = {
            "run_id": getattr(self, "run_id", f"{self.source_id}_manual"),
            "source_id": identity.source_id,
            "runner_name": identity.runner_name,
            "provider_id": identity.provider_id,
            "source_aliases_version": SCHEMA_VERSION,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "error": error,
            "rows_in": self.rows_in,
            "rows_loaded": self.rows_loaded,
            "empty_load_evidence_path": self.empty_load_evidence.path
            if isinstance(self.empty_load_evidence, EmptyLoadEvidence)
            else None,
            "empty_load_evidence_sha256": self.empty_load_evidence.sha256
            if isinstance(self.empty_load_evidence, EmptyLoadEvidence)
            else None,
            "empty_load_artifact_path": self.empty_load_evidence.artifact_path
            if isinstance(self.empty_load_evidence, EmptyLoadEvidence)
            else None,
            "empty_load_source_sha256": hashlib.sha256(
                self.empty_load_evidence.source_url.encode()
            ).hexdigest()
            if isinstance(self.empty_load_evidence, EmptyLoadEvidence)
            else None,
            **{k: v for k, v in vars(self.load_metrics).items() if not k.startswith("_")},
        }
        with self.driver.session(database=self.neo4j_database) as session:
            session.run(query, params).consume()
