from __future__ import annotations

import os
import subprocess
import sys
import threading
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from bracc_etl.evidence import (
    ContractError,
    EntityRef,
    EvidenceRef,
    FactEvent,
    ObservationRevision,
    ReferenceStatus,
    RevisionAction,
    event_identity,
    read_fact_envelope,
    serialize_fact_envelope,
)
from bracc_etl.evidence.store import EvidenceStore

SOCKET, PORT, DATABASE = sys.argv[1:4]
ARTIFACT = uuid.UUID("48f61f34-b677-4fbd-a24d-c3acb928c117")
DIGEST = "e2316b7f5c667012be8ef365497b34cb3f2bc84d603a9f60d18429e58f62ae5e"


def connect(user: str = "postgres") -> psycopg.Connection[Any]:
    return psycopg.connect(
        host=SOCKET,
        port=int(PORT),
        user=user,
        password="synthetic-only",
        dbname=DATABASE,
    )


class PostInsertFaultError(RuntimeError):
    pass


class FailAfterJournalInsert:
    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection
        self.insert_reached = False

    def transaction(self):
        return self.connection.transaction()

    def execute(self, query: str, params: Any = None):
        result = self.connection.execute(query, params)
        if query.lstrip().startswith("INSERT INTO evidence.journal"):
            self.insert_reached = True
            raise PostInsertFaultError("synthetic fault after journal INSERT executed")
        return result


def envelope(
    event_id: uuid.UUID,
    value: int,
    revision: int = 1,
    previous: str | None = None,
    action: RevisionAction = RevisionAction.ASSERT,
    locator: str = "row:1",
    event_value: Any = 2026,
    payload: Any = None,
    evidence_fields: tuple[str, ...] = (),
    evidence_refs: tuple[EvidenceRef, ...] | None = None,
    artifact_id: uuid.UUID = ARTIFACT,
    artifact_sha256: str = DIGEST,
    source_id: str = "journal-source",
    dataset_id: str = "journal-dataset",
    contract_version: str = "v1",
):
    payload_value = {"value": value} if payload is None else payload
    event = FactEvent(
        event_id,
        source_id,
        dataset_id,
        contract_version,
        EntityRef("person", status=ReferenceStatus.UNRESOLVED),
        "observed",
        {"value": event_value},
    )
    observation = ObservationRevision(
        source_id,
        dataset_id,
        contract_version,
        artifact_id,
        artifact_sha256,
        locator,
        "csv-row",
        "p1",
        payload_value if action is not RevisionAction.RETRACT else None,
        revision_no=revision,
        action=action,
        source_record_key="r1",
        supersedes_observation_id=previous,
    )
    evidence = (
        evidence_refs
        if evidence_refs is not None
        else tuple(
            EvidenceRef(
                observation.observation_id,
                artifact_id,
                artifact_sha256,
                locator,
                "csv-row",
                field_path,
            )
            for field_path in evidence_fields
        )
    )
    return read_fact_envelope(serialize_fact_envelope(event, observation, evidence=evidence))


def journal_snapshot() -> tuple[Any, ...]:
    with connect() as conn:
        locks = conn.execute(
            "SELECT event_identity FROM evidence.event_lock ORDER BY event_identity"
        ).fetchall()
        journal = conn.execute(
            "SELECT event_identity,revision_no,observation_id,canonical_bytes "
            "FROM evidence.journal ORDER BY event_identity,revision_no"
        ).fetchall()
        sources = conn.execute("SELECT id,access_class FROM control.source ORDER BY id").fetchall()
        datasets = conn.execute(
            "SELECT id,source_id,contract_version,access_class FROM control.dataset ORDER BY id"
        ).fetchall()
        partitions = conn.execute(
            "SELECT id,dataset_id FROM control.partition ORDER BY id"
        ).fetchall()
        artifacts = conn.execute(
            "SELECT id,partition_id,object_key,sha256,access_class "
            "FROM control.artifact ORDER BY id"
        ).fetchall()
        release = conn.execute(
            "SELECT release_id FROM control.active_release ORDER BY release_id"
        ).fetchall()
    return (
        tuple(journal),
        tuple(locks),
        tuple(sources),
        tuple(datasets),
        tuple(partitions),
        tuple(artifacts),
        tuple(release),
    )


def main() -> None:
    event_id = uuid.UUID("bb7170e0-c88e-4f08-992a-195edbfc263c")
    first = envelope(
        event_id,
        2026,
        payload={
            "amount": Decimal("10.25"),
            "record_id": uuid.UUID("22222222-3333-4444-5555-666666666666"),
            "as_of": date(2026, 11, 19),
        },
        evidence_fields=("attribute.amount", "relation.source"),
    )
    conn = connect()
    conn.execute("GRANT bracc_evidence_journal TO postgres")
    conn.commit()
    conn.execute("SET ROLE bracc_evidence_journal")
    store = EvidenceStore(conn)
    chain = store.append(first)
    assert len(chain.revisions) == 1
    assert len(store.append(first).revisions) == 1
    assert (
        conn.execute(
            "SELECT count(*) FROM evidence.journal WHERE event_identity=%s",
            (event_identity(first.event),),
        ).fetchone()[0]
        == 1
    )
    assert store.envelopes(first.event) == (first,)
    conn.commit()
    conn.close()

    # Fault only after the real PostgreSQL INSERT returns; adapter transaction must roll back.
    before_fault = journal_snapshot()
    assert any(row[0] == event_identity(first.event) for row in before_fault[0])
    active_release_baseline_empty = not before_fault[-1]
    assert active_release_baseline_empty
    runtime_fault_conn = connect("bracc_u05_runtime")
    fault_driver = FailAfterJournalInsert(runtime_fault_conn)
    fault_envelope = envelope(
        uuid.UUID("a4e9cdb0-9f83-4fd2-a913-591a93cbbac0"),
        88,
        locator="fault-after-insert",
    )
    try:
        EvidenceStore(fault_driver).append(fault_envelope)
    except PostInsertFaultError as exc:
        assert str(exc) == "synthetic fault after journal INSERT executed"
        assert fault_driver.insert_reached is True
    else:
        raise AssertionError("post-INSERT transaction fault was not raised")
    runtime_fault_conn.close()
    assert journal_snapshot() == before_fault

    # Authenticate as an actual login inheriting the narrow journal role.
    runtime_event = envelope(
        uuid.UUID("093dbdb9-dc1e-4ab8-9bb8-1b30baadd740"),
        77,
        locator="runtime-authenticated",
    )
    with connect("bracc_u05_runtime") as runtime_conn:
        identity = runtime_conn.execute(
            "SELECT session_user,current_user,rolsuper,rolcreaterole,rolcreatedb "
            "FROM pg_roles WHERE rolname=current_user"
        ).fetchone()
        assert identity == (
            "bracc_u05_runtime",
            "bracc_u05_runtime",
            False,
            False,
            False,
        )
        rights = runtime_conn.execute(
            "SELECT has_table_privilege(current_user,'evidence.journal','UPDATE'), "
            "has_table_privilege(current_user,'evidence.journal','DELETE'), "
            "has_table_privilege(current_user,'evidence.journal','TRUNCATE')"
        ).fetchone()
        assert rights == (False, False, False)
        assert len(EvidenceStore(runtime_conn).append(runtime_event).revisions) == 1
        assert EvidenceStore(runtime_conn).envelopes(runtime_event.event) == (runtime_event,)
    runtime_baseline = journal_snapshot()
    mutation_refusals = 0
    with connect("bracc_u05_runtime") as runtime_conn:
        for statement in (
            "UPDATE evidence.journal SET revision_no=99",
            "DELETE FROM evidence.journal",
            "TRUNCATE evidence.journal",
        ):
            try:
                runtime_conn.execute(statement)
            except psycopg.Error as exc:
                assert exc.sqlstate == "42501"
                assert exc.diag.message_primary == "permission denied for table journal"
                mutation_refusals += 1
                runtime_conn.rollback()
            else:
                raise AssertionError(f"runtime journal mutation succeeded: {statement}")
        assert EvidenceStore(runtime_conn).envelopes(runtime_event.event) == (runtime_event,)
    assert journal_snapshot() == runtime_baseline
    assert mutation_refusals == 3

    # Two authenticated runtime logins race an exact replay under a held row lock.
    replay_race = envelope(
        uuid.UUID("80a3c8e3-7d82-49c7-909c-15b3ab963221"),
        66,
        locator="exact-replay-race",
    )
    writer_inserted = threading.Event()
    allow_writer_commit = threading.Event()
    replay_started = threading.Event()
    backend_pids: dict[str, int] = {}
    replay_outcomes: list[str] = []
    replay_errors: list[BaseException] = []

    def holding_writer() -> None:
        try:
            with connect("bracc_u05_runtime") as race_conn:
                backend_pids["writer"] = race_conn.execute("SELECT pg_backend_pid()").fetchone()[0]
                with race_conn.transaction():
                    EvidenceStore(race_conn).append(replay_race)
                    writer_inserted.set()
                    if not allow_writer_commit.wait(timeout=15):
                        raise TimeoutError("writer commit barrier timed out")
                replay_outcomes.append("writer-committed")
        except BaseException as exc:
            replay_errors.append(exc)
            writer_inserted.set()

    def exact_replayer() -> None:
        try:
            with connect("bracc_u05_runtime") as race_conn:
                backend_pids["replayer"] = race_conn.execute("SELECT pg_backend_pid()").fetchone()[
                    0
                ]
                replay_started.set()
                chain = EvidenceStore(race_conn).append(replay_race)
                assert len(chain.revisions) == 1
                replay_outcomes.append("replay-accepted")
        except BaseException as exc:
            replay_errors.append(exc)
            replay_started.set()

    writer_thread = threading.Thread(target=holding_writer)
    replay_thread = threading.Thread(target=exact_replayer)
    writer_thread.start()
    assert writer_inserted.wait(timeout=10)
    assert not replay_errors, repr(replay_errors)
    replay_thread.start()
    assert replay_started.wait(timeout=10)
    assert backend_pids["writer"] != backend_pids["replayer"]
    lock_observed = False
    lock_deadline = __import__("time").monotonic() + 10
    while __import__("time").monotonic() < lock_deadline:
        with connect() as observer:
            wait = observer.execute(
                "SELECT wait_event_type,pg_blocking_pids(pid) FROM pg_stat_activity WHERE pid=%s",
                (backend_pids["replayer"],),
            ).fetchone()
        if wait is not None and wait[0] == "Lock" and backend_pids["writer"] in wait[1]:
            lock_observed = True
            break
        __import__("time").sleep(0.02)
    try:
        assert lock_observed, "exact replay did not wait on writer's PostgreSQL lock"
    finally:
        allow_writer_commit.set()
    writer_thread.join(timeout=20)
    replay_thread.join(timeout=20)
    assert not writer_thread.is_alive() and not replay_thread.is_alive()
    assert not replay_errors, repr(replay_errors)
    assert sorted(replay_outcomes) == ["replay-accepted", "writer-committed"]
    with connect() as conn:
        replay_rows = conn.execute(
            "SELECT canonical_bytes FROM evidence.journal WHERE event_identity=%s",
            (event_identity(replay_race.event),),
        ).fetchall()
    assert len(replay_rows) == 1
    assert bytes(replay_rows[0][0]) == replay_race.canonical_bytes

    refusal_diagnostics: list[str] = []

    def expect_refusal(candidate, diagnostic: str) -> None:
        before = journal_snapshot()
        with connect() as refused_conn:
            refused_conn.execute("SET ROLE bracc_evidence_journal")
            try:
                EvidenceStore(refused_conn).append(candidate)
            except ContractError as exc:
                assert str(exc) == diagnostic
            else:
                raise AssertionError(f"catalog refusal accepted: {candidate.observation}")
        assert journal_snapshot() == before
        refusal_diagnostics.append(diagnostic)

    mismatch = "evidence artifact catalog source, dataset, or contract mismatch"
    identity_mismatch = "evidence artifact catalog identity or hash mismatch"
    missing_primary = envelope(
        uuid.uuid4(), 1, artifact_id=uuid.UUID("aaaaaaaa-0000-4000-8000-000000000001")
    )
    expect_refusal(missing_primary, identity_mismatch)
    wrong_primary_hash = envelope(uuid.uuid4(), 1, artifact_sha256="0" * 64)
    expect_refusal(wrong_primary_hash, identity_mismatch)
    wrong_primary_source = envelope(uuid.uuid4(), 1, source_id="journal-source-alt")
    expect_refusal(wrong_primary_source, mismatch)
    wrong_primary_dataset = envelope(uuid.uuid4(), 1, dataset_id="journal-dataset-other")
    expect_refusal(wrong_primary_dataset, mismatch)
    wrong_primary_contract = envelope(uuid.uuid4(), 1, contract_version="v2")
    expect_refusal(wrong_primary_contract, mismatch)

    def with_reference(event_id: uuid.UUID, artifact_id: uuid.UUID, digest: str = DIGEST):
        base = envelope(event_id, 1)
        ref = EvidenceRef(
            base.observation.observation_id,
            artifact_id,
            digest,
            base.observation.record_locator,
            base.observation.locator_kind,
            "attribute.value",
        )
        return envelope(event_id, 1, evidence_refs=(ref,))

    missing_reference = with_reference(
        uuid.uuid4(), uuid.UUID("aaaaaaaa-0000-4000-8000-000000000002")
    )
    expect_refusal(missing_reference, identity_mismatch)
    wrong_reference_hash = with_reference(uuid.uuid4(), ARTIFACT, "f" * 64)
    expect_refusal(wrong_reference_hash, identity_mismatch)
    wrong_reference_source = with_reference(
        uuid.uuid4(), uuid.UUID("48f61f34-b677-4fbd-a24d-c3acb928c118")
    )
    expect_refusal(wrong_reference_source, mismatch)
    wrong_reference_dataset = with_reference(
        uuid.uuid4(), uuid.UUID("48f61f34-b677-4fbd-a24d-c3acb928c11a")
    )
    expect_refusal(wrong_reference_dataset, mismatch)
    wrong_reference_contract = with_reference(
        uuid.uuid4(), uuid.UUID("48f61f34-b677-4fbd-a24d-c3acb928c119")
    )
    expect_refusal(wrong_reference_contract, mismatch)

    changed_event = FactEvent(
        first.event.event_id,
        first.event.source_id,
        first.event.dataset_id,
        first.event.dataset_contract_version,
        first.event.subject,
        first.event.predicate,
        {"value": 999},
        event_at=first.event.event_at,
        valid_from=first.event.valid_from,
        valid_to=first.event.valid_to,
        date_precision=first.event.date_precision,
        source_record_key=first.event.source_record_key,
    )
    changed_event_replay = read_fact_envelope(
        serialize_fact_envelope(changed_event, first.observation, evidence=first.evidence)
    )
    expect_refusal(
        changed_event_replay,
        "replayed observation ID has conflicting envelope content",
    )
    altered_evidence = EvidenceRef(
        first.observation.observation_id,
        ARTIFACT,
        DIGEST,
        first.observation.record_locator,
        first.observation.locator_kind,
        "attribute.other",
    )
    changed_replay = envelope(
        event_id,
        2026,
        payload=first.observation.payload,
        evidence_refs=(altered_evidence,),
    )
    expect_refusal(
        changed_replay,
        "replayed observation ID has conflicting envelope content",
    )

    # A new interpreter and a newly authenticated connection must recover typed history.
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            "import os,sys; from bracc_etl.evidence.store import EvidenceStore; "
            "import psycopg; "
            "from bracc_etl.evidence import EntityRef,FactEvent,ReferenceStatus; "
            "c=psycopg.connect(host=sys.argv[1],port=int(sys.argv[2]),"
            "user='postgres',password='synthetic-only',dbname=sys.argv[3]); "
            "event=FactEvent(__import__('uuid').UUID("
            "'bb7170e0-c88e-4f08-992a-195edbfc263c'),"
            "'journal-source','journal-dataset','v1',"
            "EntityRef('person',status=ReferenceStatus.UNRESOLVED),"
            "'observed',{'value':2026}); "
            "store=EvidenceStore(c); h=store.event_history(event); "
            "envs=store.envelopes(event); "
            "print(os.getpid()); "
            "[print(item.canonical_bytes.hex()) for item in envs]",
            SOCKET,
            PORT,
            DATABASE,
        ],
        check=True,
        text=True,
        capture_output=True,
        env={**__import__("os").environ, "PYTHONPATH": str(Path(__file__).parents[2] / "src")},
    )
    child_lines = child.stdout.splitlines()
    child_pid = int(child_lines[0])
    assert child_pid != os.getpid()
    restarted = tuple(read_fact_envelope(bytes.fromhex(line)) for line in child_lines[1:])
    assert len(restarted) == 1
    assert restarted[0] == first
    assert restarted[0].canonical_bytes == first.canonical_bytes
    assert restarted[0].evidence == first.evidence
    assert [item.field_path for item in restarted[0].evidence] == [
        "attribute.amount",
        "relation.source",
    ]
    assert restarted[0].observation.payload["amount"] == Decimal("10.25")
    assert restarted[0].observation.payload["record_id"] == uuid.UUID(
        "22222222-3333-4444-5555-666666666666"
    )
    assert restarted[0].observation.payload["as_of"] == date(2026, 11, 19)
    conn = connect()
    conn.execute("SET ROLE bracc_evidence_journal")
    store = EvidenceStore(conn)
    for statement in (
        "UPDATE evidence.journal SET revision_no=99",
        "DELETE FROM evidence.journal",
        "TRUNCATE evidence.journal",
    ):
        try:
            conn.execute(statement)
        except psycopg.Error as exc:
            assert exc.sqlstate == "42501"
            assert exc.diag.message_primary == "permission denied for table journal"
            conn.rollback()
            conn.execute("SET ROLE bracc_evidence_journal")
        else:
            raise AssertionError(f"journal mutation unexpectedly accepted: {statement}")
    assert store.event_history(first.event).revisions == (first.observation,)
    conn.close()

    # Same pair/value with distinct explicit business UUIDs remain separate.
    second_event = uuid.UUID("7dce3104-281c-45f6-9cc8-5a9987a54a41")
    with connect() as conn:
        store = EvidenceStore(conn)
        store.append(
            envelope(
                second_event,
                2026,
                payload=first.observation.payload,
                evidence_fields=("attribute.amount", "relation.source"),
            )
        )
        rows = conn.execute(
            "SELECT event_id,observation_id FROM evidence.journal "
            "WHERE record_locator='row:1' AND revision_no=1"
        ).fetchall()
        assert {row[0] for row in rows} == {event_id, second_event}
        assert len(rows) == 2
        assert rows[0][1] == rows[1][1] == first.observation.observation_id

    first_envelope = first
    event = first_envelope.event
    original = first_envelope.observation
    corrected_event = FactEvent(
        event.event_id,
        event.source_id,
        event.dataset_id,
        event.dataset_contract_version,
        event.subject,
        event.predicate,
        {"value": 2027},
        event_at=event.event_at,
        valid_from=event.valid_from,
        valid_to=event.valid_to,
        date_precision=event.date_precision,
        source_record_key=event.source_record_key,
    )
    corrected = ObservationRevision(
        original.source_id,
        original.dataset_id,
        original.dataset_contract_version,
        original.artifact_id,
        original.artifact_sha256,
        original.record_locator,
        original.locator_kind,
        original.payload_schema_version,
        {"value": 2027},
        revision_no=2,
        action=RevisionAction.CORRECT,
        source_record_key=original.source_record_key,
        supersedes_observation_id=original.observation_id,
    )
    retracted = ObservationRevision(
        original.source_id,
        original.dataset_id,
        original.dataset_contract_version,
        original.artifact_id,
        original.artifact_sha256,
        original.record_locator,
        original.locator_kind,
        original.payload_schema_version,
        None,
        revision_no=3,
        action=RevisionAction.RETRACT,
        source_record_key=original.source_record_key,
        supersedes_observation_id=corrected.observation_id,
    )
    corrected_envelope = read_fact_envelope(serialize_fact_envelope(corrected_event, corrected))
    retracted_envelope = read_fact_envelope(serialize_fact_envelope(corrected_event, retracted))
    with connect() as conn:
        store = EvidenceStore(conn)
        assert [r.revision_no for r in store.append(corrected_envelope).revisions] == [1, 2]
        assert [r.revision_no for r in store.append(retracted_envelope).revisions] == [
            1,
            2,
            3,
        ]
        history = store.event_history(event)
        assert [r.action for r in history.revisions] == [
            RevisionAction.ASSERT,
            RevisionAction.CORRECT,
            RevisionAction.RETRACT,
        ]
        assert store.event_history(event) == history
        full_history = store.envelopes(event)
        assert len(full_history) == 3
        assert [item.event for item in full_history] == [event, corrected_event, corrected_event]
        assert [item.canonical_bytes for item in full_history] == [
            first.canonical_bytes,
            corrected_envelope.canonical_bytes,
            retracted_envelope.canonical_bytes,
        ]
        replay_bad = ObservationRevision(
            original.source_id,
            original.dataset_id,
            original.dataset_contract_version,
            original.artifact_id,
            original.artifact_sha256,
            original.record_locator,
            original.locator_kind,
            original.payload_schema_version,
            {"value": 2027},
            revision_no=3,
            action=RevisionAction.CORRECT,
            source_record_key=original.source_record_key,
            supersedes_observation_id=retracted.observation_id,
        )
        try:
            store.append(read_fact_envelope(serialize_fact_envelope(event, replay_bad)))
        except ContractError as exc:
            assert str(exc) == "retracted record cannot accept another revision"
        else:
            raise AssertionError("revision after retract accepted")
        assert store.event_history(event) == history

    # Different locator isolates history for revision exercise.
    def isolated(
        value: int,
        revision: int = 1,
        predecessor: str | None = None,
        action: RevisionAction = RevisionAction.ASSERT,
    ):
        item = envelope(
            uuid.UUID("c027e3af-9156-4832-a619-c80b274cc411"), value, revision, predecessor, action
        )
        event = item.event
        obs = item.observation
        from dataclasses import replace

        obs = replace(obs, record_locator="revision-row")
        return read_fact_envelope(serialize_fact_envelope(event, obs))

    one = isolated(1)
    with connect() as conn:
        store = EvidenceStore(conn)
        store.append(one)
        two_obs = ObservationRevision(
            one.observation.source_id,
            one.observation.dataset_id,
            one.observation.dataset_contract_version,
            ARTIFACT,
            DIGEST,
            "revision-row",
            "csv-row",
            "p1",
            {"value": 2},
            revision_no=2,
            action=RevisionAction.CORRECT,
            source_record_key="r1",
            supersedes_observation_id=one.observation.observation_id,
        )
        two = read_fact_envelope(serialize_fact_envelope(one.event, two_obs))
        chain = store.append(two)
        assert [item.revision_no for item in chain.revisions] == [1, 2]
        bad = ObservationRevision(
            one.observation.source_id,
            one.observation.dataset_id,
            "v1",
            ARTIFACT,
            DIGEST,
            "revision-row",
            "csv-row",
            "p1",
            {"value": 3},
            revision_no=3,
            action=RevisionAction.CORRECT,
            source_record_key="r1",
            supersedes_observation_id="0" * 64,
        )
        try:
            store.append(read_fact_envelope(serialize_fact_envelope(one.event, bad)))
        except ValueError as exc:
            assert "revision must supersede the exact prior observation" in str(exc)
        else:
            raise AssertionError("wrong predecessor accepted")
        history = store.event_history(one.event)
        assert [item.revision_no for item in history.revisions] == [1, 2]

    # Competing revisions use two real connections and a barrier; unique revision key arbitrates.
    race_base = envelope(uuid.UUID("32f45c03-796a-4cb1-8f0f-a27f2e750b03"), 0, locator="race-row")
    with connect() as conn:
        EvidenceStore(conn).append(race_base)
    gate = threading.Barrier(2)
    outcomes: list[str] = []
    errors: list[BaseException] = []
    candidates = []
    for value in (1, 2):
        obs = ObservationRevision(
            "journal-source",
            "journal-dataset",
            "v1",
            ARTIFACT,
            DIGEST,
            "race-row",
            "csv-row",
            "p1",
            {"value": value},
            revision_no=2,
            action=RevisionAction.CORRECT,
            source_record_key="r1",
            supersedes_observation_id=race_base.observation.observation_id,
        )
        ev = race_base.event
        candidates.append(read_fact_envelope(serialize_fact_envelope(ev, obs)))

    def racer(item) -> None:
        try:
            with connect() as conn:
                gate.wait(timeout=10)
                EvidenceStore(conn).append(item)
                outcomes.append("accepted")
        except ValueError as exc:
            if "revision must supersede the exact prior observation" in str(exc):
                outcomes.append("conflict")
            else:
                errors.append(exc)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=racer, args=(item,)) for item in candidates]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)
    assert all(not thread.is_alive() for thread in threads) and not errors, repr(errors)
    assert sorted(outcomes) == ["accepted", "conflict"], outcomes
    with connect() as conn:
        rows = conn.execute(
            "SELECT revision_no FROM evidence.journal WHERE event_identity=%s ORDER BY revision_no",
            (event_identity(race_base.event),),
        ).fetchall()
        assert [row[0] for row in rows] == [1, 2]
    assert len(refusal_diagnostics) == 12
    assert refusal_diagnostics.count(identity_mismatch) == 4
    assert refusal_diagnostics.count(mismatch) == 6
    assert (
        refusal_diagnostics.count("replayed observation ID has conflicting envelope content") == 2
    )
    print(
        f"runner PID {os.getpid()}, restart PID {child_pid}, "
        f"catalog_refusals={len(refusal_diagnostics)}, "
        f"rollback_after_insert={fault_driver.insert_reached}, "
        f"release_baseline_empty={active_release_baseline_empty}, "
        f"runtime_role={identity}, denied_mutations={mutation_refusals}, "
        f"exact_replay_lock={lock_observed}, backend_pids={backend_pids}, "
        "divergent_revision_race=PASS"
    )


if __name__ == "__main__":
    main()
