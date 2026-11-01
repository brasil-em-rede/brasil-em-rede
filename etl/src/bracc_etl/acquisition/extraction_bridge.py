from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from bracc_etl.acquisition.client import Manifest

from bracc_etl.extraction import (
    ExtractionContractError,
    ExtractionResult,
    SourceAdapterSpec,
    open_extraction,
)


def build_extraction_manifest(
    root: Path,
    acquisition_manifest: Manifest,
    adapter: SourceAdapterSpec,
    *,
    run_id: str,
    source_id: str,
    source_url: str,
    assignments: dict[str, dict[str, tuple[str, str, str]]],
    page_urls: dict[str, str],
    output_path: Path | None = None,
) -> Path:
    """Build a distinct F06 manifest from explicit caller-owned bindings.

    URLs are used only to verify F05's URL fingerprints and are never serialized.
    """
    if not all(isinstance(value, str) and value for value in (run_id, source_id, source_url)):
        raise ExtractionContractError("explicit run/source/url identity is required")
    if source_id != adapter.source_id or source_url != adapter.expected_source_url:
        raise ExtractionContractError("source or URL differs from adapter policy")
    if acquisition_manifest.source_id != source_id:
        raise ExtractionContractError("acquisition source mismatch")

    expected_ids = {population.population_id for population in adapter.populations}
    if len(expected_ids) != len(adapter.populations) or set(assignments) != expected_ids:
        raise ExtractionContractError("population assignment coverage mismatch")
    assigned_pages = {page for assigned in assignments.values() for page, _, _ in assigned.values()}
    if set(page_urls) != assigned_pages or any(
        not isinstance(url, str) or not url for url in page_urls.values()
    ):
        raise ExtractionContractError("explicit per-page URL provenance is required")

    root = Path(root)
    root_fd: int | None = None
    try:
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as exc:
        raise ExtractionContractError("root must be an actual directory, not a symlink") from exc
    finally:
        if root_fd is not None:
            os.close(root_fd)
    populations: list[dict[str, Any]] = []
    seen_pages: set[str] = set()
    for spec in adapter.populations:
        assigned = assignments[spec.population_id]
        expected_artifacts = dict(spec.required_artifacts)
        if set(assigned) - set(expected_artifacts):
            raise ExtractionContractError("unexpected artifact assignment")
        if spec.required and set(assigned) != set(expected_artifacts):
            raise ExtractionContractError("required population artifact missing")

        artifacts = []
        for artifact_id, (page, schema_id, schema_version) in assigned.items():
            if not page or Path(page).name != page or page in {".", ".."} or "\x00" in page:
                raise ExtractionContractError("unsafe artifact path")
            if (schema_id, schema_version) != (spec.schema_id, spec.schema_version):
                raise ExtractionContractError("schema assignment differs from adapter")
            fmt = expected_artifacts.get(artifact_id)
            if fmt is None:
                raise ExtractionContractError("artifact assignment differs from adapter")
            if page in seen_pages:
                raise ExtractionContractError("duplicate page assignment")
            seen_pages.add(page)

            record = acquisition_manifest.pages.get(page)
            expected_url_hash = hashlib.sha256(page_urls[page].encode()).hexdigest()
            if not isinstance(record, dict) or record.get("source_sha256") != expected_url_hash:
                raise ExtractionContractError("page URL provenance mismatch")
            checkpoint_hash = record.get("sha256")
            checkpoint_bytes = record.get("bytes")
            if (
                not isinstance(checkpoint_hash, str)
                or len(checkpoint_hash) != 64
                or type(checkpoint_bytes) is not int
                or checkpoint_bytes < 0
            ):
                raise ExtractionContractError("invalid acquisition checkpoint")

            candidate = root / page
            try:
                descriptor = os.open(
                    candidate,
                    os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                )
                with os.fdopen(descriptor, "rb") as stream:
                    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                        raise ExtractionContractError("artifact is not a regular file")
                    digest = hashlib.sha256()
                    byte_length = 0
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(chunk)
                        byte_length += len(chunk)
                    actual_hash = digest.hexdigest()
            except OSError as exc:
                raise ExtractionContractError("artifact missing or inaccessible") from exc
            if checkpoint_hash != actual_hash or checkpoint_bytes != byte_length:
                raise ExtractionContractError("artifact checkpoint mismatch")

            artifacts.append(
                {
                    "artifact_id": artifact_id,
                    "relative_path": page,
                    "format": fmt,
                    "sha256": actual_hash,
                    "byte_length": byte_length,
                    "schema_id": schema_id,
                    "schema_version": schema_version,
                }
            )
        populations.append({"population_id": spec.population_id, "artifacts": artifacts})

    output = output_path or (root / "f06-extraction-manifest.json")
    if output.name in {".", ".."} or output.parent.resolve(strict=True) != root:
        raise ExtractionContractError("extraction manifest must be a direct child of root")
    if output.exists() or output.is_symlink():
        raise ExtractionContractError("refusing to overwrite extraction manifest")
    obj = {
        "manifest_schema_version": 1,
        "adapter_id": adapter.adapter_id,
        "adapter_version": adapter.adapter_version,
        "run_id": run_id,
        "source_id": source_id,
        "source_url": source_url,
        "populations": populations,
    }
    descriptor = os.open(
        output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(obj, stream, ensure_ascii=False, sort_keys=True)
    return output


def open_acquired_extraction(
    root: Path,
    manifest_path: Path,
    adapter: SourceAdapterSpec,
    *,
    run_id: str,
    source_id: str,
    source_url: str,
) -> tuple[ExtractionResult, tuple[Any, ...]]:
    """Open the generated manifest through F06's authoritative verifier."""
    return open_extraction(
        root, manifest_path, adapter, run_id=run_id, source_id=source_id, source_url=source_url
    )
