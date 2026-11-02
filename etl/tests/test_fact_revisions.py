from __future__ import annotations

import dataclasses
import hashlib
import json
import uuid
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from bracc_etl.acquisition.catalog import RawArtifactRef
from bracc_etl.acquisition.client import Manifest
from bracc_etl.evidence import (
    ContractError,
    EntityRef,
    EvidenceRef,
    FactEvent,
    IdentityUnresolvedError,
    ObservationRevision,
    ReferenceStatus,
    RevisionAction,
    RevisionChain,
    apply_revision,
    canonical_json,
    decode_canonical_json,
    event_identity,
    read_fact_envelope,
)

ROOT = Path(__file__).parents[2]
SCHEMA_PATH = ROOT / "config/unification/fact_contracts/fact-envelope.schema.json"
ARTIFACT_ID = uuid.UUID("48f61f34-b677-4fbd-a24d-c3acb928c117")
EVENT_ID = uuid.UUID("bb7170e0-c88e-4f08-992a-195edbfc263c")
DIGEST = hashlib.sha256(b"inline synthetic fixture").hexdigest()


def artifact_ref() -> RawArtifactRef:
    return RawArtifactRef(
        "source-demo", None, "dataset-demo", ARTIFACT_ID, DIGEST, "p1", f"raw/{DIGEST}"
    )


def assertion(
    *,
    payload: Any = None,
    locator: str = "row:2",
    locator_kind: str = "csv-row",
    observed_at: datetime | None = None,
    source_record_key: str = "record-1",
) -> ObservationRevision:
    return ObservationRevision(
        source_id="source-demo",
        dataset_id="dataset-demo",
        dataset_contract_version="v1",
        artifact_id=artifact_ref().artifact_id,
        artifact_sha256=artifact_ref().artifact_sha256,
        record_locator=locator,
        locator_kind=locator_kind,
        payload_schema_version="p1",
        payload={"status": "approved", "amount": Decimal("10.50")} if payload is None else payload,
        source_record_key=source_record_key,
        observed_at=observed_at,
    )


def fact(event_id: uuid.UUID = EVENT_ID) -> FactEvent:
    return FactEvent(
        event_id,
        "source-demo",
        "dataset-demo",
        "v1",
        EntityRef("person", status=ReferenceStatus.UNRESOLVED),
        "awarded_to",
        EntityRef(
            "organization",
            uuid.UUID("3520a136-5661-4388-846d-1a654b4d7ca0"),
            status=ReferenceStatus.CANDIDATE_REFERENCE,
        ),
        event_at=datetime(2026, 1, 2, tzinfo=UTC),
        valid_from=None,
        valid_to=None,
        date_precision="instant",
    )


def test_schema_is_valid_json_and_contract_matches_valid_python_envelope() -> None:
    schema = json.loads(SCHEMA_PATH.read_text())
    item = assertion()
    event = fact()
    envelope = {
        "schema_version": "1",
        "event": {
            "event_id": str(event.event_id),
            "source_id": event.source_id,
            "dataset_id": event.dataset_id,
            "dataset_contract_version": event.dataset_contract_version,
            "subject": {
                "entity_type": event.subject.entity_type,
                "entity_id": None,
                "status": "unresolved",
            },
            "predicate": event.predicate,
            "object": {
                "entity_id": str(event.object.entity_id),
                "entity_type": event.object.entity_type,
                "status": "candidate_reference",
            },
            "event_at": event.event_at.isoformat(),
            "valid_from": None,
            "valid_to": None,
            "date_precision": event.date_precision,
        },
        "observation": {
            "source_id": item.source_id,
            "dataset_id": item.dataset_id,
            "dataset_contract_version": item.dataset_contract_version,
            "artifact_id": str(item.artifact_id),
            "artifact_sha256": item.artifact_sha256,
            "record_locator": item.record_locator,
            "locator_kind": item.locator_kind,
            "payload_schema_version": item.payload_schema_version,
            "payload": json.loads(
                canonical_json({"status": "approved", "amount": Decimal("10.50")})
            ),
            "revision_no": 1,
            "action": "assert",
        },
    }
    assert schema["$schema"].endswith("2020-12/schema")
    assert schema["additionalProperties"] is False
    assert set(envelope) == set(schema["required"])
    assert set(envelope["observation"]) <= set(schema["$defs"]["observationRevision"]["properties"])
    assert set(envelope["event"]) <= set(schema["$defs"]["factEvent"]["properties"])
    assert decode_canonical_json(canonical_json(envelope)) == envelope
    assert item.observation_id == assertion().observation_id


def test_artifact_manifest_value_boundary_stays_pure() -> None:
    ref = artifact_ref()
    manifest = Manifest(
        Path("synthetic-manifest.json"),
        ref.source_id,
        ref.partition_id,
        {"page.csv": {"sha256": ref.artifact_sha256, "bytes": 24}},
    )
    assert manifest.source_id == ref.source_id
    assert ref.artifact_id == ARTIFACT_ID and ref.artifact_sha256 == DIGEST


def test_complete_provenance_and_opaque_locator_kinds() -> None:
    assert (
        assertion(locator="csv-row:2", locator_kind="csv-row").observation_id
        != assertion(locator="/items/1", locator_kind="json-pointer").observation_id
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", ""),
        ("dataset_id", ""),
        ("dataset_contract_version", ""),
        ("artifact_sha256", "A" * 64),
        ("artifact_sha256", "z" * 64),
        ("record_locator", ""),
        ("locator_kind", ""),
        ("payload_schema_version", ""),
        ("artifact_id", "not-a-uuid"),
        ("revision_no", True),
    ],
)
def test_invalid_required_provenance_and_types_are_rejected(field: str, value: Any) -> None:
    from dataclasses import replace

    with pytest.raises(ContractError):
        replace(assertion(), **{field: value})


def test_unknown_optional_provenance_stays_null() -> None:
    item = assertion()
    assert (
        item.source_release
        is item.parser_version
        is item.normalizer_version
        is item.observed_at
        is None
    )


def test_event_ids_are_explicit_and_distinct_even_for_same_business_values() -> None:
    one, two = fact(), fact(uuid.UUID("7dce3104-281c-45f6-9cc8-5a9987a54a41"))
    assert event_identity(one) != event_identity(two)
    assert one.object == two.object and one.event_at == two.event_at


def test_observation_id_is_deterministic_and_excludes_clock_and_run_metadata() -> None:
    first = assertion(observed_at=datetime(2026, 1, 1, tzinfo=UTC))
    second = assertion(observed_at=datetime(2026, 2, 1, tzinfo=UTC))
    assert first.observation_id == second.observation_id
    assert assertion(payload={"changed": True}).observation_id != first.observation_id


def test_frozen_value_objects_and_deep_immutability() -> None:
    item = assertion(payload={"nested": {"value": 1}})
    with pytest.raises(FrozenInstanceError):
        item.revision_no = 2  # type: ignore[misc]
    with pytest.raises(TypeError):
        item.payload["nested"]["value"] = 3


def test_correction_retraction_chain_and_exact_replay() -> None:
    first = assertion()
    corrected = ObservationRevision(
        first.source_id,
        first.dataset_id,
        first.dataset_contract_version,
        first.artifact_id,
        first.artifact_sha256,
        first.record_locator,
        first.locator_kind,
        first.payload_schema_version,
        {"status": "corrected"},
        revision_no=2,
        action=RevisionAction.CORRECT,
        source_record_key=first.source_record_key,
        supersedes_observation_id=first.observation_id,
    )
    retracted = ObservationRevision(
        first.source_id,
        first.dataset_id,
        first.dataset_contract_version,
        first.artifact_id,
        first.artifact_sha256,
        first.record_locator,
        first.locator_kind,
        first.payload_schema_version,
        None,
        revision_no=3,
        action=RevisionAction.RETRACT,
        source_record_key=first.source_record_key,
        supersedes_observation_id=corrected.observation_id,
    )
    chain = apply_revision(RevisionChain(), first)
    chain = apply_revision(chain, first)
    assert len(chain.revisions) == 1
    chain = apply_revision(chain, corrected)
    chain = apply_revision(chain, retracted)
    assert [item.action for item in chain.revisions] == [
        RevisionAction.ASSERT,
        RevisionAction.CORRECT,
        RevisionAction.RETRACT,
    ]
    assert chain.revisions[0].payload == first.payload
    assert apply_revision(chain, None) is chain


def test_wrong_predecessor_or_record_and_payload_retraction_rejected() -> None:
    first = assertion()
    with pytest.raises(ContractError):
        ObservationRevision(
            first.source_id,
            first.dataset_id,
            first.dataset_contract_version,
            first.artifact_id,
            first.artifact_sha256,
            first.record_locator,
            first.locator_kind,
            first.payload_schema_version,
            {"forbidden": True},
            revision_no=2,
            action=RevisionAction.RETRACT,
            source_record_key=first.source_record_key,
            supersedes_observation_id=first.observation_id,
        )
    chain = RevisionChain((first,))
    bad = ObservationRevision(
        first.source_id,
        first.dataset_id,
        first.dataset_contract_version,
        first.artifact_id,
        first.artifact_sha256,
        first.record_locator,
        first.locator_kind,
        first.payload_schema_version,
        {"bad": True},
        revision_no=2,
        action=RevisionAction.CORRECT,
        source_record_key="different",
        supersedes_observation_id=first.observation_id,
    )
    with pytest.raises(ContractError):
        apply_revision(chain, bad)
    bad_predecessor = dataclasses.replace(
        bad, source_record_key=first.source_record_key, supersedes_observation_id="a" * 64
    )
    with pytest.raises(ContractError):
        apply_revision(chain, bad_predecessor)


def test_invalid_temporal_interval_naive_time_and_nonfinite_json_rejected() -> None:
    from dataclasses import replace

    with pytest.raises(ContractError, match="valid_to must not precede valid_from"):
        dataclasses.replace(
            fact(),
            event_at=None,
            date_precision="day",
            valid_from=date(2026, 2, 1),
            valid_to=date(2026, 1, 1),
        )
    with pytest.raises(ContractError):
        replace(fact(), event_at=datetime(2026, 1, 1))
    with pytest.raises(ContractError):
        canonical_json({"not-finite": float("nan")})
    with pytest.raises(ContractError):
        canonical_json({"not-finite": Decimal("Infinity")})


def test_entity_reference_status_is_typed_and_never_resolved_implicitly() -> None:
    assert EntityRef("person").status is ReferenceStatus.UNRESOLVED
    with pytest.raises(ContractError):
        EntityRef("person", uuid.uuid4(), status=ReferenceStatus.UNRESOLVED)
    with pytest.raises(ContractError):
        EntityRef("person", status="exact_reference")


def test_evidence_ref_requires_valid_artifact_provenance() -> None:
    item = assertion()
    ref = EvidenceRef(
        item.observation_id,
        item.artifact_id,
        item.artifact_sha256,
        item.record_locator,
        item.locator_kind,
    )
    assert ref.field_path is None


def test_python_schema_contract_agreement_and_negative_schema_cases() -> None:
    schema = json.loads(SCHEMA_PATH.read_text())
    props = schema["$defs"]["observationRevision"]["properties"]
    item = assertion()
    envelope = {
        "source_id": item.source_id,
        "dataset_id": item.dataset_id,
        "dataset_contract_version": item.dataset_contract_version,
        "artifact_id": str(item.artifact_id),
        "artifact_sha256": item.artifact_sha256,
        "record_locator": item.record_locator,
        "locator_kind": item.locator_kind,
        "payload_schema_version": item.payload_schema_version,
        "payload": json.loads(canonical_json({"status": "approved", "amount": Decimal("10.50")})),
        "revision_no": item.revision_no,
        "action": item.action.value,
        "source_record_key": item.source_record_key,
        "source_release": item.source_release,
        "parser_version": item.parser_version,
        "normalizer_version": item.normalizer_version,
        "observed_at": item.observed_at,
        "supersedes_observation_id": item.supersedes_observation_id,
    }
    assert set(envelope) <= set(props)
    assert set(schema["$defs"]["observationRevision"]["required"]) <= set(envelope)
    assert schema["$defs"]["sha256"]["pattern"] == "^[0-9a-f]{64}$"
    assert schema["$defs"]["observationRevision"]["additionalProperties"] is False
    assert schema["$defs"]["factEvent"]["additionalProperties"] is False
    assert schema["additionalProperties"] is False
    unknown = dict(envelope, surprise=True)
    assert "surprise" not in props
    assert item.action is RevisionAction.ASSERT and item.revision_no == 1
    assert item.supersedes_observation_id is None
    with pytest.raises(ContractError):
        ObservationRevision(
            item.source_id,
            item.dataset_id,
            item.dataset_contract_version,
            item.artifact_id,
            "X" * 64,
            item.record_locator,
            item.locator_kind,
            item.payload_schema_version,
            item.payload,
        )
    assert "surprise" not in envelope and unknown["surprise"] is True


def test_fact_event_rejects_mixed_temporal_kinds_with_contract_error() -> None:
    with pytest.raises(ContractError, match="same temporal kind"):
        dataclasses.replace(
            fact(), valid_from=date(2026, 1, 1),
            valid_to=datetime(2026, 1, 2, tzinfo=UTC),
        )


def test_fact_event_date_precision_is_closed_and_matches_temporal_values() -> None:
    with pytest.raises(ContractError, match="day precision"):
        dataclasses.replace(fact(), event_at=datetime(2026, 1, 2, tzinfo=UTC), date_precision="day")
    with pytest.raises(ContractError, match="instant precision"):
        dataclasses.replace(fact(), event_at=date(2026, 1, 2), date_precision="instant")
    with pytest.raises(ContractError):
        dataclasses.replace(fact(), date_precision="month")
    assert dataclasses.replace(fact(), event_at=None, date_precision="unknown").event_at is None
    day_event = dataclasses.replace(fact(), event_at=date(2026, 1, 2), date_precision="day")
    assert day_event.event_at == date(2026, 1, 2)
    with pytest.raises(ContractError):
        dataclasses.replace(fact(), source_record_key=" \t")


def test_fact_event_object_is_deeply_immutable_and_copied() -> None:
    source = {"nested": [{"value": 1}]}
    item = dataclasses.replace(fact(), object=source)
    revision = assertion(payload=source)
    revision_id = revision.observation_id
    source["nested"][0]["value"] = 2
    assert item.object["nested"][0]["value"] == 1
    assert revision.payload["nested"][0]["value"] == 1
    assert revision.observation_id == revision_id
    with pytest.raises(TypeError):
        item.object["nested"][0]["value"] = 3
    with pytest.raises(ContractError):
        EntityRef("   ")
    with pytest.raises(ContractError):
        EntityRef("person", namespace=" \t")


def test_typed_wire_json_is_lossless_and_collision_safe() -> None:
    from decimal import Decimal

    payload = {
        "decimal": Decimal("10.50"),
        "reserved-looking": {"$bracc": ["decimal", "10.50"]},
        "uuid": ARTIFACT_ID,
        "uuid-text": str(ARTIFACT_ID),
        "date": date(2026, 1, 2),
        "date-text": "2026-01-02",
        "datetime": datetime(2026, 1, 2, 3, 4, tzinfo=timezone(timedelta(hours=-3))),
        "datetime-text": "2026-01-02T03:04:00-03:00",
        "nested": [None, True, 3, 3.25, ["x"]],
    }
    wire = canonical_json(payload)
    restored = decode_canonical_json(wire)
    assert restored == payload
    assert type(restored["decimal"]) is Decimal
    assert isinstance(restored["reserved-looking"], dict)
    assert canonical_json(restored["decimal"]) != canonical_json(restored["reserved-looking"])
    assert type(restored["date"]) is date and type(restored["date-text"]) is str
    assert type(restored["datetime"]) is datetime and type(restored["datetime-text"]) is str
    assert type(restored["uuid"]) is uuid.UUID and type(restored["uuid-text"]) is str
    assert decode_canonical_json(canonical_json([payload, payload])) == [payload, payload]
    with pytest.raises(ContractError, match="format version"):
        decode_canonical_json('["br-acc-typed-json/v2", ["null", null]]')
    with pytest.raises(ContractError):
        decode_canonical_json('["br-acc-typed-json/v1", ["int", true]]')


def test_event_identity_survives_record_key_and_contract_version_evolution() -> None:
    base = fact()
    changed = dataclasses.replace(base, predicate="corrected_predicate")
    next_contract = dataclasses.replace(base, dataset_contract_version="v2")
    other_record = dataclasses.replace(base, source_record_key="record-2")
    assert event_identity(base) == event_identity(changed)
    assert event_identity(base) == event_identity(next_contract)
    assert event_identity(base) == event_identity(other_record)
    assert event_identity(base) != event_identity(dataclasses.replace(base, event_id=uuid.uuid4()))
    observation = assertion()
    assert dataclasses.replace(
        observation, dataset_contract_version="v2"
    ).observation_id != observation.observation_id
    assert dataclasses.replace(
        observation, source_record_key="record-2"
    ).observation_id != observation.observation_id


def _public_envelope() -> dict[str, Any]:
    item = assertion()
    event = fact()
    return {
        "schema_version": "1",
        "event": {
            "event_id": str(event.event_id),
            "source_id": event.source_id,
            "dataset_id": event.dataset_id,
            "dataset_contract_version": event.dataset_contract_version,
            "source_record_key": event.source_record_key,
            "subject": {"entity_type": "person", "status": "unresolved"},
            "predicate": event.predicate,
            "object": {
                "entity_type": "organization",
                "entity_id": "3520a136-5661-4388-846d-1a654b4d7ca0",
                "status": "candidate_reference",
            },
            "event_at": "2026-01-02T00:00:00+00:00",
            "valid_from": None,
            "valid_to": None,
            "date_precision": event.date_precision,
        },
        "observation": {
            "source_id": item.source_id,
            "dataset_id": item.dataset_id,
            "dataset_contract_version": item.dataset_contract_version,
            "artifact_id": str(item.artifact_id),
            "artifact_sha256": item.artifact_sha256,
            "record_locator": item.record_locator,
            "locator_kind": item.locator_kind,
            "payload_schema_version": item.payload_schema_version,
            "payload": json.loads(canonical_json(item.payload)),
            "revision_no": 1,
            "action": "assert",
        },
    }


def test_public_json_boundary_returns_validated_dtos_and_canonical_bytes() -> None:
    wire = json.dumps(_public_envelope())
    validated = read_fact_envelope(wire)
    assert isinstance(validated.event, FactEvent)
    assert isinstance(validated.observation, ObservationRevision)
    assert validated.observation.payload == assertion().payload
    assert validated.canonical_bytes == read_fact_envelope(wire).canonical_bytes


@pytest.mark.parametrize(
    "wire",
    [
        '{"schema_version":"1","schema_version":"1"}',
        '{"schema_version":"1","nested":{"x":1,"x":2}}',
        '{"schema_version":"1","x":NaN}',
        '{"schema_version":"1","x":Infinity}',
        '{"schema_version":"1","event":{},"observation":{},"unexpected":true}',
    ],
)
def test_public_json_boundary_fails_closed_on_ambiguous_or_invalid_json(wire: str) -> None:
    with pytest.raises(ContractError):
        read_fact_envelope(wire)


def test_public_json_boundary_rejects_semantically_invalid_event_before_consumer() -> None:
    envelope = _public_envelope()
    envelope["event"]["date_precision"] = "day"
    envelope["event"]["valid_from"] = "2026-02-01"
    envelope["event"]["valid_to"] = "2026-01-01"
    before = RevisionChain()
    with pytest.raises(ContractError):
        read_fact_envelope(json.dumps(envelope))
    assert before.revisions == ()


def test_public_json_boundary_marks_missing_business_uuid_unresolved() -> None:
    envelope = _public_envelope()
    del envelope["event"]["event_id"]
    with pytest.raises(IdentityUnresolvedError, match="explicit producer event_id"):
        read_fact_envelope(json.dumps(envelope))


def test_public_json_boundary_rejects_noncanonical_payload_tags() -> None:
    envelope = _public_envelope()
    envelope["observation"]["payload"] = ["br-acc-typed-json/v1", ["float", "1"]]
    with pytest.raises(ContractError):
        read_fact_envelope(json.dumps(envelope))


@pytest.mark.parametrize(
    "entries",
    [
        [["same", ["int", 1]], ["same", ["int", 2]]],
        [["z", ["int", 1]], ["a", ["int", 2]]],
    ],
)
def test_public_json_boundary_rejects_duplicate_or_unsorted_tagged_object_keys(
    entries: list[list[Any]],
) -> None:
    envelope = _public_envelope()
    envelope["observation"]["payload"] = ["br-acc-typed-json/v1", ["object", entries]]
    with pytest.raises(ContractError):
        read_fact_envelope(json.dumps(envelope))


@pytest.mark.parametrize(
    "field",
    ["source_id", "dataset_id", "dataset_contract_version"],
)
def test_public_consumer_rejects_event_observation_namespace_mismatch_before_transition(
    field: str,
) -> None:
    envelope = _public_envelope()
    envelope["observation"][field] = "different-" + field
    raw = json.dumps(envelope).encode("utf-8")
    before = RevisionChain()
    with pytest.raises(ContractError, match="event and observation namespace must match"):
        from bracc_etl.evidence import apply_serialized_revision

        apply_serialized_revision(before, raw)
    assert before.revisions == ()


def test_matching_namespace_control_reaches_public_revision_consumer() -> None:
    from bracc_etl.evidence import apply_serialized_revision

    raw = json.dumps(_public_envelope()).encode("utf-8")
    result = apply_serialized_revision(RevisionChain(), raw)
    assert len(result.revisions) == 1
    assert result.revisions[0] == read_fact_envelope(raw).observation


def test_event_identity_evolves_without_cross_record_chain_continuation() -> None:
    base_event = fact()
    base_observation = assertion()
    changed_event = dataclasses.replace(
        base_event, dataset_contract_version="v2", source_record_key="record-2"
    )
    changed_observation = dataclasses.replace(
        base_observation, dataset_contract_version="v2", source_record_key="record-2"
    )
    assert event_identity(base_event) == event_identity(changed_event)
    assert base_observation.observation_id != changed_observation.observation_id
    base_chain = apply_revision(RevisionChain(), base_observation)
    changed_chain = apply_revision(RevisionChain(), changed_observation)
    assert base_chain.revisions == (base_observation,)
    assert changed_chain.revisions == (changed_observation,)
    continuation = dataclasses.replace(
        changed_observation,
        revision_no=2,
        action=RevisionAction.CORRECT,
        supersedes_observation_id=base_observation.observation_id,
    )
    with pytest.raises(ContractError, match="revision chain cannot change record identity"):
        apply_revision(base_chain, continuation)
    assert base_chain.revisions == (base_observation,)


def test_public_dto_serializer_round_trips_through_reader_and_consumer() -> None:
    from bracc_etl.evidence import apply_serialized_revision, serialize_fact_envelope

    event = dataclasses.replace(
        fact(),
        subject=EntityRef("person", namespace="br-acc", scope="public"),
        object={
            "entity_type": "plain-value-not-reference",
            "decimal": Decimal("10.50"),
            "uuid": ARTIFACT_ID,
            "uuid_text": str(ARTIFACT_ID),
            "date": date(2026, 1, 2),
            "date_text": "2026-01-02",
            "datetime": datetime(2026, 1, 2, 3, 4, tzinfo=timezone(timedelta(hours=-3))),
            "datetime_text": "2026-01-02T03:04:00-03:00",
            "nested": [None, True, 3, 3.25, {"reserved-looking": ["decimal", "10.50"]}],
        },
    )
    observation = assertion(
        payload={
            "decimal": Decimal("10.50"),
            "uuid": ARTIFACT_ID,
            "date": date(2026, 1, 2),
            "datetime": datetime(2026, 1, 2, 3, 4, tzinfo=timezone(timedelta(hours=-3))),
            "nested": [None, True, 3, 3.25, {"entity_type": "ordinary object"}],
        }
    )
    evidence = (
        EvidenceRef(
            observation.observation_id,
            observation.artifact_id,
            observation.artifact_sha256,
            observation.record_locator,
            observation.locator_kind,
            "payload.amount",
        ),
    )
    first = serialize_fact_envelope(event, observation, evidence)
    second = serialize_fact_envelope(event, observation, evidence)
    assert isinstance(first, bytes) and first == second
    validated = read_fact_envelope(first)
    assert validated.event == event
    assert validated.observation == observation
    assert validated.evidence == evidence
    chain = apply_serialized_revision(RevisionChain(), first)
    assert chain.revisions == (observation,)

    reference_event = dataclasses.replace(
        fact(),
        subject=EntityRef(
            "person",
            uuid.UUID("816a2b20-9627-47e4-8efa-60e1fef6fb7a"),
            status=ReferenceStatus.EXACT_REFERENCE,
        ),
    )
    reference_bytes = serialize_fact_envelope(reference_event, assertion())
    restored_reference = read_fact_envelope(reference_bytes).event
    assert restored_reference.subject == reference_event.subject
    assert restored_reference.object == reference_event.object


def test_serializer_rejects_event_observation_namespace_mismatch() -> None:
    from bracc_etl.evidence import serialize_fact_envelope

    mismatched = dataclasses.replace(assertion(), dataset_id="another-dataset")
    with pytest.raises(ContractError, match="event and observation namespace must match"):
        serialize_fact_envelope(fact(), mismatched)



def test_public_reader_is_wired_to_real_revision_consumer() -> None:
    from bracc_etl.evidence import apply_serialized_revision

    envelope = _public_envelope()
    envelope["event"]["date_precision"] = "day"
    envelope["event"]["valid_from"] = "2026-02-01"
    envelope["event"]["valid_to"] = "2026-01-01"
    with pytest.raises(ContractError):
        apply_serialized_revision(RevisionChain(), json.dumps(envelope))
    envelope = _public_envelope()
    result = apply_serialized_revision(RevisionChain(), json.dumps(envelope))
    assert result.revisions == (read_fact_envelope(json.dumps(envelope)).observation,)
    envelope["observation"]["source_record_key"] = "not-the-same-record"
    bad = json.dumps(envelope)
    with pytest.raises(ContractError):
        apply_serialized_revision(result, bad)
    assert len(result.revisions) == 1
