"""Exercise publish_partition against the real Catalog and disposable PostgreSQL."""

from __future__ import annotations

import hashlib
import sys
import threading
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

import psycopg

from bracc_etl.acquisition import Manifest
from bracc_etl.acquisition.catalog import publish_partition
from bracc_etl.control.catalog import Catalog

SOCKET = sys.argv[1]
OWNER = UUID("00000000-0000-0000-0000-000000000003")


def connect() -> psycopg.Connection:
    return psycopg.connect(host=SOCKET, user="postgres", dbname="bracc_f05_lifecycle")


def make_inputs(base: Path, partition: str) -> tuple[Manifest, Path, Path]:
    artifacts = base / f"input-{partition}"
    storage = base / f"objects-{partition}"
    artifacts.mkdir()
    body = f"synthetic:{partition}".encode()
    page = "page.csv"
    (artifacts / page).write_bytes(body)
    manifest = Manifest(base / f"manifest-{partition}.json", "cnpj", partition)
    manifest.pages[page] = {"sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
    manifest.save()
    return manifest, artifacts, storage


def publish(catalog: Catalog, inputs, *, run: int, attempt: int, manifest_id: int):
    manifest, artifact_dir, storage = inputs
    return publish_partition(
        catalog,
        manifest,
        ["page.csv"],
        artifact_dir,
        storage_root=storage,
        run_id=UUID(int=run),
        idempotency_key=f"run-{run}",
        attempt_id=UUID(int=attempt),
        owner=OWNER,
        manifest_id=UUID(int=manifest_id),
        dataset_id="cnpj",
        access_class="public",
        lease_duration=timedelta(minutes=1),
    )


def main() -> None:
    with TemporaryDirectory(prefix="f05-lifecycle-") as temporary:
        base = Path(temporary)
        inputs = make_inputs(base, "2026-01")
        with connect() as conn:
            catalog = Catalog(conn)
            release_before = conn.execute(
                "SELECT count(*) FROM control.active_release"
            ).fetchone()[0]
            refs = publish(catalog, inputs, run=101, attempt=201, manifest_id=301)
            assert len(refs) == 1
            row = conn.execute(
                "SELECT m.id, m.partition_id, a.id, a.object_key, a.sha256, a.access_class "
                "FROM control.partition_manifest m "
                "JOIN control.manifest_artifact ma ON ma.manifest_id=m.id "
                "JOIN control.artifact a ON a.id=ma.artifact_id WHERE m.id=%s",
                (UUID(int=301),),
            ).fetchone()
            assert row is not None and row[0:3] == (UUID(int=301), "2026-01", refs[0].artifact_id)
            assert (
                row[3] == refs[0].object_key
                and row[4] == refs[0].artifact_sha256
                and row[5] == "public"
            )
            assert (inputs[2] / refs[0].object_key).read_bytes() == (
                inputs[1] / "page.csv"
            ).read_bytes()

            # Existing low-level registration contract remains independently usable.
            # Roll back a second adapter publication on conflicting manifest identity;
            # only immutable bytes may remain, never partial attempt/artifact rows.
            before_artifacts = conn.execute("SELECT count(*) FROM control.artifact").fetchone()[0]
            before_attempts = conn.execute("SELECT count(*) FROM control.run_attempt").fetchone()[0]
            rollback_inputs = make_inputs(base, "2026-04")
            try:
                publish(catalog, rollback_inputs, run=102, attempt=202, manifest_id=301)
            except psycopg.Error:
                pass
            else:
                raise AssertionError("conflicting manifest identity was accepted")
            assert (
                conn.execute("SELECT count(*) FROM control.artifact").fetchone()[0]
                == before_artifacts
            )
            assert (
                conn.execute("SELECT count(*) FROM control.run_attempt").fetchone()[0]
                == before_attempts
            )
            assert (inputs[2] / refs[0].object_key).is_file()

        # Two real Catalog instances synchronize immediately before lease acquisition.
        race_inputs = make_inputs(base, "2026-02")
        gate = threading.Barrier(2)
        outcomes: list[str] = []
        errors: list[BaseException] = []

        def racer(i: int) -> None:
            try:
                with connect() as conn:
                    catalog = Catalog(conn)
                    gate.wait(timeout=10)
                    publish(catalog, race_inputs, run=110 + i, attempt=210 + i, manifest_id=310 + i)
                    outcomes.append("published")
            except ValueError as exc:
                if "lease unavailable" not in str(exc):
                    errors.append(exc)
                else:
                    outcomes.append("lease-rejected")
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=racer, args=(i,)) for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
        assert all(not thread.is_alive() for thread in threads)
        assert not errors, repr(errors)
        assert sorted(outcomes) == ["lease-rejected", "published"]
        with connect() as conn:
            assert (
                conn.execute(
                    "SELECT count(*) FROM control.partition_manifest WHERE partition_id='2026-02'"
                ).fetchone()[0]
                == 1
            )
            assert (
                conn.execute(
                    "SELECT count(*) FROM control.artifact WHERE partition_id='2026-02'"
                ).fetchone()[0]
                == 1
            )

        # Expire a genuine acquired lease using fixture-controlled DB state, then
        # prove the Catalog's fenced finalizer rejects the old token.
        with connect() as conn:
            catalog = Catalog(conn)
            token = catalog.lease("2026-03", OWNER, timedelta(minutes=1))
            assert token is not None
            conn.execute(
                "INSERT INTO control.run(id,idempotency_key) VALUES (%s,%s)",
                (UUID(int=401), "expired-run"),
            )
            catalog.start_attempt(
                UUID(int=401), "expired-run", UUID(int=402), "2026-03", OWNER, token
            )
            conn.execute(
                "UPDATE control.partition_lease "
                "SET expires_at=clock_timestamp()-interval '1 second' "
                "WHERE partition_id='2026-03'"
            )
            conn.commit()
            try:
                with catalog.transaction():
                    catalog.manifest(
                        UUID(int=403), "2026-03", [UUID(int=999)], OWNER, token, UUID(int=402)
                    )
            except psycopg.Error as exc:
                assert "stale lease" in str(exc)
            else:
                raise AssertionError("expired lease finalized a manifest")
            assert (
                conn.execute(
                    "SELECT count(*) FROM control.partition_manifest WHERE partition_id='2026-03'"
                ).fetchone()[0]
                == 0
            )
            assert (
                conn.execute("SELECT count(*) FROM control.active_release").fetchone()[0]
                == release_before
            )

    print(
        "F05 real Catalog lifecycle: finalized lineage, rollback boundary, "
        "competing lease fencing, expired lease rejection passed"
    )


if __name__ == "__main__":
    main()
