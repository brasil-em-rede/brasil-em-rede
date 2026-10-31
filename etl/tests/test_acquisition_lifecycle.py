from __future__ import annotations

from hashlib import sha256
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from pathlib import Path

import pytest

from bracc_etl.acquisition import Manifest
from bracc_etl.acquisition.catalog import publish_partition


class Connection:
    def __init__(self) -> None:
        self.rows = {}
        self.inserted = []

    def execute(self, query: str, params: tuple):
        if query.startswith("SELECT id"):
            self.result = [
                row for row in self.rows.values() if row[0] == params[0] or row[2] == params[1]
            ]
        else:
            self.inserted.append(params)
            self.rows[params[0]] = params
        return self

    def fetchall(self):
        return self.result


class Catalog:
    def __init__(self) -> None:
        self.connection = Connection()
        self.calls: list[tuple] = []

    def lease(self, *args):
        self.calls.append(("lease", *args))
        return 7

    def start_attempt(self, *args):
        self.calls.append(("attempt", *args))

    def manifest(self, *args):
        self.calls.append(("manifest", *args))

    def transaction(self):
        from contextlib import nullcontext

        return nullcontext()

    def artifact(self, *args):
        self.calls.append(("artifact", *args))


def test_publishes_complete_partition_with_fenced_attempt(tmp_path: Path) -> None:
    raw = b"synthetic page\n"
    page = tmp_path / "page.csv"
    page.write_bytes(raw)
    manifest = Manifest(tmp_path / "manifest.json", "cnpj", "2026-01")
    manifest.pages[page.name] = {"sha256": sha256(raw).hexdigest(), "bytes": len(raw)}
    manifest.save()
    catalog = Catalog()
    result = publish_partition(
        catalog,
        manifest,
        [page.name],
        tmp_path,
        storage_root=tmp_path / "objects",
        run_id=UUID(int=1),
        idempotency_key="run-1",
        attempt_id=UUID(int=2),
        owner=UUID(int=3),
        manifest_id=UUID(int=4),
        dataset_id="cnpj",
        access_class="public",
    )
    assert len(result) == 1
    assert [call[0] for call in catalog.calls] == ["lease", "attempt", "artifact", "manifest"]
    assert catalog.calls[-1] == (
        "manifest",
        UUID(int=4),
        "2026-01",
        [result[0].artifact_id],
        UUID(int=3),
        7,
        UUID(int=2),
    )


def test_rejects_incomplete_page_set_before_lease(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "manifest.json", "cnpj", "2026-01")
    manifest.save()
    catalog = Catalog()
    with pytest.raises(ValueError):
        publish_partition(
            catalog,
            manifest,
            ["missing.csv"],
            tmp_path,
            storage_root=tmp_path / "objects",
            run_id=UUID(int=1),
            idempotency_key="run-1",
            attempt_id=UUID(int=2),
            owner=UUID(int=3),
            manifest_id=UUID(int=4),
            dataset_id="cnpj",
            access_class="public",
        )
    assert catalog.calls == []
