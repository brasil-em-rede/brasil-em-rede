"""Validated bridge from acquisition manifests to the U03 raw artifact catalog.

This records source identity and provenance only; entity identity resolution belongs
to downstream U02 consumers and is intentionally not performed here.
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
import uuid
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uuid import UUID

from bracc_etl.source_aliases import SourceIdentity, source_aliases

from .client import Manifest

_SAFE_SEGMENT = re.compile(r"[A-Za-z0-9_-]+\Z")

if TYPE_CHECKING:
    from bracc_etl.control.catalog import Catalog


@dataclass(frozen=True)
class RawArtifactRef:
    """Immutable acquisition provenance envelope for one verified raw artifact."""

    source_id: str
    provider_source_id: str | None
    dataset_id: str
    artifact_id: UUID
    artifact_sha256: str
    partition_id: str
    object_key: str


def resolve_manifest_source(manifest: Manifest) -> SourceIdentity:
    """Resolve a legacy or canonical manifest source ID through F01's versioned map."""
    return source_aliases().resolve(manifest.source_id)


def register_raw_artifact(
    catalog: Catalog,
    manifest: Manifest,
    page: str,
    artifact_path: Path,
    *,
    storage_root: Path,
    artifact_id: UUID,
    partition_id: str,
    dataset_id: str,
    access_class: str,
) -> RawArtifactRef:
    """Publish verified bytes under an immutable raw key, then register that object."""
    identity = resolve_manifest_source(manifest)
    if not page or Path(page).name != page or page in {".", ".."}:
        raise ValueError("invalid artifact page")
    saved = Manifest.open(manifest.path, manifest.source_id, manifest.partition)
    record = saved.pages.get(page)
    if not record or not artifact_path.is_file():
        raise ValueError("manifest page or artifact missing")
    digest, size = _digest(artifact_path)
    if digest != record.get("sha256") or size != record.get("bytes"):
        raise ValueError("artifact bytes disagree with acquisition manifest")
    if manifest.partition != partition_id:
        raise ValueError("manifest partition mismatch")
    if not _SAFE_SEGMENT.fullmatch(dataset_id) or not _SAFE_SEGMENT.fullmatch(partition_id):
        raise ValueError("invalid catalog path segment")
    key = f"raw/{identity.source_id}/{dataset_id}/{partition_id}/{digest}"
    target = storage_root.joinpath(*key.split("/"))
    _publish_immutable(artifact_path, target, digest, size)
    existing = catalog.connection.execute(
        "SELECT id, partition_id, object_key, sha256, access_class FROM control.artifact "
        "WHERE id = %s OR object_key = %s",
        (artifact_id, key),
    ).fetchall()
    expected = (artifact_id, partition_id, key, digest, access_class)
    if existing and (len(existing) != 1 or tuple(existing[0]) != expected):
        raise ValueError("artifact ID already registered with different identity")
    if not existing:
        catalog.artifact(artifact_id, partition_id, key, digest, access_class)
    return RawArtifactRef(
        source_id=identity.source_id,
        provider_source_id=identity.provider_id,
        dataset_id=dataset_id,
        artifact_id=artifact_id,
        artifact_sha256=digest,
        partition_id=partition_id,
        object_key=key,
    )


def publish_partition(
    catalog: Catalog,
    manifest: Manifest,
    pages: list[str],
    artifact_directory: Path,
    *,
    storage_root: Path,
    run_id: UUID,
    idempotency_key: str,
    attempt_id: UUID,
    owner: UUID,
    manifest_id: UUID,
    dataset_id: str,
    access_class: str,
    lease_duration: timedelta = timedelta(minutes=5),
) -> list[RawArtifactRef]:
    """Register a complete, verified partition under a fenced U03 attempt.

    Immutable objects publish before database rows; rejection may leave unreferenced
    objects but never indicates success. The caller owns the connection lifetime.
    """
    if not pages or len(pages) != len(set(pages)):
        raise ValueError("partition requires a non-empty unique page set")
    saved = Manifest.open(manifest.path, manifest.source_id, manifest.partition)
    if set(saved.pages) != set(pages):
        raise ValueError("page set must exactly match the complete acquisition manifest")
    for page in pages:
        if not page or Path(page).name != page or page in {".", ".."}:
            raise ValueError("invalid artifact page")
        record = saved.pages[page]
        path = artifact_directory / page
        if not path.is_file() or _digest(path) != (record.get("sha256"), record.get("bytes")):
            raise ValueError("artifact bytes disagree with acquisition manifest")
    token = catalog.lease(manifest.partition, owner, lease_duration)
    if token is None:
        raise ValueError("partition lease unavailable")
    refs: list[RawArtifactRef] = []
    with catalog.transaction():
        catalog.start_attempt(run_id, idempotency_key, attempt_id, manifest.partition, owner, token)
        for page in pages:
            artifact_id = uuid.uuid5(
                attempt_id, f"{manifest.source_id}/{manifest.partition}/{page}"
            )
            refs.append(
                register_raw_artifact(
                    catalog,
                    saved,
                    page,
                    artifact_directory / page,
                    storage_root=storage_root,
                    artifact_id=artifact_id,
                    partition_id=manifest.partition,
                    dataset_id=dataset_id,
                    access_class=access_class,
                )
            )
        catalog.manifest(
            manifest_id,
            manifest.partition,
            [ref.artifact_id for ref in refs],
            owner,
            token,
            attempt_id,
        )
    return refs


def _digest(path: Path) -> tuple[str, int]:
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
            size += len(block)
    return hasher.hexdigest(), size


def _matches(path: Path, digest: str, size: int) -> bool:
    try:
        if not path.is_file() or path.is_symlink():
            return False
        return _digest(path) == (digest, size)
    except OSError:
        return False


def _publish_immutable(source: Path, target: Path, digest: str, size: int) -> None:
    """Stage a verified copy and atomically publish without replacing an existing key."""
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        if _matches(target, digest, size):
            return
        raise ValueError("existing storage object differs from manifest artifact")

    fd, temporary_name = tempfile.mkstemp(prefix=f".{digest}.", suffix=".tmp", dir=target.parent)
    temporary = Path(temporary_name)
    try:
        hasher = hashlib.sha256()
        copied = 0
        with os.fdopen(fd, "wb") as output, source.open("rb") as input_stream:
            for block in iter(lambda: input_stream.read(1024 * 1024), b""):
                output.write(block)
                hasher.update(block)
                copied += len(block)
            output.flush()
            os.fsync(output.fileno())
        if hasher.hexdigest() != digest or copied != size:
            raise ValueError("artifact changed while publishing")
        try:
            # The staged copy is immutable; the source file itself is never linked.
            os.link(temporary, target)
        except FileExistsError:
            if not _matches(target, digest, size):
                raise ValueError("existing storage object differs from manifest artifact") from None
        if not _matches(target, digest, size):
            raise ValueError("published storage object differs from manifest artifact")
        directory_fd = os.open(target.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
