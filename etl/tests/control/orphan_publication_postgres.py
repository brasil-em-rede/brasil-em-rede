"""Exercise orphan publication and release fencing on disposable PostgreSQL."""
from __future__ import annotations

import hashlib
import sys
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast
from uuid import UUID

import psycopg

from bracc_etl.acquisition import Manifest
from bracc_etl.acquisition.catalog import publish_partition
from bracc_etl.control.catalog import Catalog

SOCKET = sys.argv[1]
DATABASE = "bracc_m0_u03_orphans"
OWNER = UUID("00000000-0000-0000-0000-000000000003")


def connect() -> psycopg.Connection[Any]:
    return psycopg.connect(host=SOCKET, user="postgres", dbname=DATABASE)


def identifier(value: int) -> UUID:
    return UUID(int=value)


def make_inputs(base: Path, partition: str) -> tuple[Manifest, Path, Path]:
    artifact_dir = base / f"source-{partition}"
    storage_root = base / f"objects-{partition}"
    artifact_dir.mkdir()
    content = f"synthetic orphan regression: {partition}".encode()
    (artifact_dir / "page.csv").write_bytes(content)
    manifest = Manifest(base / f"manifest-{partition}.json", "cnpj", partition)
    manifest.pages["page.csv"] = {
        "sha256": hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
    }
    manifest.save()
    return manifest, artifact_dir, storage_root


def publish(catalog: Catalog, source: tuple[Manifest, Path, Path], number: int) -> None:
    manifest, artifact_dir, storage_root = source
    publish_partition(
        catalog, manifest, ["page.csv"], artifact_dir,
        storage_root=storage_root, run_id=identifier(number),
        idempotency_key=f"synthetic-run-{number}", attempt_id=identifier(number + 1_000),
        owner=OWNER, manifest_id=identifier(number + 2_000), dataset_id="cnpj",
        access_class="public", lease_duration=timedelta(minutes=2),
    )


def row_counts(connection: psycopg.Connection[Any]) -> tuple[int, int, int]:
    tables = ("artifact", "partition_manifest", "run_attempt")
    values = [
        fetch_required(connection, f"SELECT count(*) FROM control.{table}")[0]
        for table in tables
    ]
    return values[0], values[1], values[2]


def release_members(connection: psycopg.Connection[Any], release: UUID) -> list[tuple[str, UUID]]:
    return connection.execute(
        "SELECT m.partition_id, m.id FROM control.release_manifest rm "
        "JOIN control.partition_manifest m ON m.id = rm.manifest_id "
        "WHERE rm.release_id = %s ORDER BY m.partition_id, m.id", (release,),
    ).fetchall()


def fetch_required(
    connection: psycopg.Connection[Any],
    query: str,
    params: tuple[Any, ...] = (),
) -> tuple[Any, ...]:
    row = connection.execute(query, params).fetchone()
    assert row is not None
    return cast("tuple[Any, ...]", row)


def current_token(connection: psycopg.Connection[Any], partition: str) -> int:
    row = fetch_required(
        connection,
        "SELECT token FROM control.partition_lease WHERE partition_id = %s",
        (partition,),
    )
    return int(row[0])


def expect_database_rejection(
    action: Any,
    expected_sqlstate: str,
    expected_message: str,
    failure_message: str,
) -> None:
    try:
        with connect() as connection:
            action(Catalog(connection))
    except psycopg.Error as error:
        actual = (error.sqlstate, error.diag.message_primary)
        expected = (expected_sqlstate, expected_message)
        assert actual == expected, (
            f"{failure_message}: expected SQLSTATE/message {expected!r}, received {actual!r}"
        )
    else:
        raise AssertionError(failure_message)


def main() -> None:
    with TemporaryDirectory(prefix="u03-orphan-") as temporary:
        base = Path(temporary)
        with connect() as connection:
            catalog = Catalog(connection)
            baseline_input = make_inputs(base, "2026-01")
            publish(catalog, baseline_input, 101)
            baseline_manifest = fetch_required(
                connection,
                "SELECT id FROM control.partition_manifest WHERE partition_id = '2026-01'",
            )[0]
            catalog.quality(identifier(3_101), baseline_manifest, True, {"synthetic": True})
            catalog.projection(
                identifier(4_101), identifier(5_101), baseline_manifest, "synthetic-v1"
            )
            connection.execute("SET ROLE bracc_control_projector")
            catalog.acknowledge(identifier(5_101))
            connection.execute("RESET ROLE")
            baseline_release = identifier(6_101)
            catalog.bundle(
                baseline_release, "synthetic", "v1", "v1",
                [("2026-01", baseline_manifest)],
            )
            baseline_token = current_token(connection, "2026-01")
            catalog.promote(baseline_release, None, "2026-01", OWNER, baseline_token)
            assert catalog.active_release() == baseline_release
            baseline_members = release_members(connection, baseline_release)
            assert baseline_members == [("2026-01", baseline_manifest)]
            connection.commit()

        orphan_input = make_inputs(base, "2026-02")
        with connect() as connection:
            before_counts = row_counts(connection)
        with connect() as connection:
            catalog = Catalog(connection)
            original_manifest = catalog.manifest

            def reject_finalization(*args: Any, **kwargs: Any) -> None:
                raise RuntimeError("synthetic finalization fault after artifact registration")

            catalog.manifest = reject_finalization  # type: ignore[method-assign]
            try:
                publish(catalog, orphan_input, 102)
            except RuntimeError as error:
                assert str(error) == "synthetic finalization fault after artifact registration"
            else:
                raise AssertionError("injected finalization fault was not reached")
            finally:
                catalog.manifest = original_manifest  # type: ignore[method-assign]
        orphan_files = [
            path for path in (orphan_input[2] / "raw").rglob("*") if path.is_file()
        ]
        assert len(orphan_files) == 1
        orphan_digest = hashlib.sha256(orphan_files[0].read_bytes()).hexdigest()
        assert orphan_files[0].name == orphan_digest
        assert orphan_files[0].read_bytes() == (orphan_input[1] / "page.csv").read_bytes()
        with connect() as connection:
            assert row_counts(connection) == before_counts
            assert fetch_required(
                connection,
                "SELECT count(*) FROM control.partition_manifest WHERE partition_id = '2026-02'",
            )[0] == 0
            assert fetch_required(
                connection,
                "SELECT count(*) FROM control.run_attempt WHERE id = %s",
                (identifier(1_102),),
            )[0] == 0
            catalog = Catalog(connection)
            assert catalog.active_release() == baseline_release
            assert release_members(connection, baseline_release) == baseline_members

        candidate_input = make_inputs(base, "2026-03")
        with connect() as connection:
            catalog = Catalog(connection)
            publish(catalog, candidate_input, 103)
            candidate_manifest = fetch_required(
                connection,
                "SELECT id FROM control.partition_manifest WHERE partition_id = '2026-03'",
            )[0]
            catalog.quality(identifier(3_103), candidate_manifest, True, {"synthetic": True})
            catalog.projection(
                identifier(4_103), identifier(5_103), candidate_manifest, "synthetic-v1"
            )
            candidate_release = identifier(6_103)
            catalog.bundle(
                candidate_release, "synthetic", "v1", "v1",
                [("2026-03", candidate_manifest)],
            )
            candidate_token = current_token(connection, "2026-03")

        def promote_pending(candidate_catalog: Catalog) -> None:
            candidate_catalog.promote(
                candidate_release, baseline_release, "2026-03", OWNER, candidate_token
            )

        expect_database_rejection(
            promote_pending,
            "P0001",
            "release incomplete",
            "candidate with pending outbox promoted",
        )
        with connect() as connection:
            catalog = Catalog(connection)
            assert catalog.active_release() == baseline_release
            assert release_members(connection, baseline_release) == baseline_members
            connection.execute("SET ROLE bracc_control_projector")
            catalog.acknowledge(identifier(5_103))
            connection.execute("RESET ROLE")
            catalog.promote(
                candidate_release, baseline_release, "2026-03", OWNER, candidate_token
            )
            assert catalog.active_release() == candidate_release
            assert release_members(connection, candidate_release) == [
                ("2026-03", candidate_manifest)
            ]

        def promote_bad_cas(candidate_catalog: Catalog) -> None:
            candidate_catalog.promote(
                baseline_release, None, "2026-01", OWNER, baseline_token
            )

        expect_database_rejection(
            promote_bad_cas,
            "P0001",
            "release changed",
            "wrong previous release passed CAS",
        )
        with connect() as connection:
            assert Catalog(connection).active_release() == candidate_release

        with connect() as connection:
            connection.execute(
                "UPDATE control.partition_lease SET expires_at = clock_timestamp() "
                "- interval '1 second' WHERE partition_id = '2026-03'"
            )
            connection.commit()

        with connect() as connection:
            lease = fetch_required(
                connection,
                "SELECT owner, token, expires_at <= clock_timestamp() "
                "FROM control.partition_lease WHERE partition_id = '2026-03'",
            )
            assert lease[0] == OWNER, f"unexpected lease owner: {lease[0]!r}"
            assert lease[1] == candidate_token, f"unexpected lease token: {lease[1]!r}"
            assert lease[2] is True, "database clock does not show an expired lease"

        def promote_stale(candidate_catalog: Catalog) -> None:
            candidate_catalog.promote(
                candidate_release, candidate_release, "2026-03", OWNER, candidate_token
            )

        expect_database_rejection(
            promote_stale,
            "P0001",
            "stale lease",
            "expired fencing token promoted release",
        )
        with connect() as connection:
            assert Catalog(connection).active_release() == candidate_release
            assert release_members(connection, baseline_release) == baseline_members

    print(
        "PASS: baseline release preserved; injected finalization fault left SHA-256-verified "
        "orphan bytes and no partial rows; pending outbox and bad CAS rejected; authorized "
        "projector ACK enabled promotion; expired token rejected"
    )


if __name__ == "__main__":
    main()
