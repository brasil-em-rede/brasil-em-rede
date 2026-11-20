from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from bracc_etl.evidence import (
    ContractError,
    EntityRef,
    EvidenceRef,
    FactEvent,
    ObservationRevision,
    ReferenceStatus,
    event_identity,
    read_fact_envelope,
    serialize_fact_envelope,
)

try:
    from bracc_etl.evidence.store import EvidenceStore
except ImportError:
    EvidenceStore = None

ARTIFACT_ID = uuid.UUID("48f61f34-b677-4fbd-a24d-c3acb928c117")
ARTIFACT_SHA = hashlib.sha256(b"synthetic fixture").hexdigest()
EVENT_ID = uuid.UUID("bb7170e0-c88e-4f08-992a-195edbfc263c")


def envelope(
    *,
    event_id: uuid.UUID = EVENT_ID,
    locator: str = "row:1",
    payload: Any = None,
    evidence: tuple[EvidenceRef, ...] = (),
):
    value = {"status": "approved", "amount": Decimal("10.50")} if payload is None else payload
    event = FactEvent(
        event_id,
        "journal-source",
        "journal-dataset",
        "v1",
        EntityRef("person", status=ReferenceStatus.UNRESOLVED),
        "observed",
        {"value": 2026},
    )
    observation = ObservationRevision(
        "journal-source",
        "journal-dataset",
        "v1",
        ARTIFACT_ID,
        ARTIFACT_SHA,
        locator,
        "csv-row",
        "p1",
        value,
        source_record_key="record-1",
        observed_at=datetime(2026, 1, 2, tzinfo=UTC),
    )
    return read_fact_envelope(serialize_fact_envelope(event, observation, evidence=evidence))


def test_event_identity_does_not_collapse_same_observation_hash() -> None:
    one = envelope(event_id=EVENT_ID)
    two = envelope(event_id=uuid.UUID("7dce3104-281c-45f6-9cc8-5a9987a54a41"))
    assert one.observation.observation_id == two.observation.observation_id
    assert event_identity(one.event) != event_identity(two.event)


def test_stale_typed_envelope_cannot_disagree_with_raw_canonical_bytes() -> None:
    if EvidenceStore is None:
        pytest.skip("optional psycopg control extra is not installed")
    good = envelope()
    altered = FactEvent(
        uuid.uuid4(),
        "journal-source",
        "journal-dataset",
        "v1",
        good.event.subject,
        good.event.predicate,
        good.event.object,
    )
    forged = type(good)(altered, good.observation, good.evidence, good.canonical_bytes)
    with pytest.raises(
        ContractError, match="canonical envelope bytes do not match validated values"
    ):
        EvidenceStore(None).append(forged)  # type: ignore[arg-type]


def test_top_level_evidence_reference_preserves_field_path() -> None:
    item = envelope()
    ref = EvidenceRef(
        item.observation.observation_id,
        ARTIFACT_ID,
        ARTIFACT_SHA,
        "row:1",
        "csv-row",
        "relation.source",
    )
    result = read_fact_envelope(
        serialize_fact_envelope(item.event, item.observation, evidence=(ref,))
    )
    assert result.evidence == (ref,)
    wrong = EvidenceRef("0" * 64, ARTIFACT_ID, ARTIFACT_SHA, "row:1", "csv-row", "attribute.value")
    with pytest.raises(ContractError):
        EvidenceStore(None)._bind_catalog(
            read_fact_envelope(
                serialize_fact_envelope(item.event, item.observation, evidence=(wrong,))
            )
        )


class _TransactionBoundaryReachedError(RuntimeError):
    pass


class _InertConnection:
    def __init__(self) -> None:
        self.transaction_calls = 0

    def transaction(self) -> None:
        self.transaction_calls += 1
        raise _TransactionBoundaryReachedError("inert connection transaction boundary reached")


def _typed_envelope(value: Any):
    base = envelope(payload={"value": value})
    event = replace(base.event, object={"value": value})
    return read_fact_envelope(
        serialize_fact_envelope(event, base.observation, evidence=base.evidence)
    )


@pytest.mark.parametrize("value", [True, 1, Decimal("1")], ids=["bool", "int", "decimal"])
def test_valid_typed_values_reach_the_connection(value: Any) -> None:
    if EvidenceStore is None:
        pytest.skip("optional psycopg control extra is not installed")
    connection = _InertConnection()
    with pytest.raises(
        _TransactionBoundaryReachedError,
        match="inert connection transaction boundary reached",
    ):
        EvidenceStore(connection).append(_typed_envelope(value))
    assert connection.transaction_calls == 1


def test_append_accepts_publicly_valid_envelope_with_optional_fields_omitted() -> None:
    if EvidenceStore is None:
        pytest.skip("optional psycopg control extra is not installed")
    canonical = _typed_envelope(1)
    document = json.loads(canonical.canonical_bytes)
    for field in ("event_at", "valid_from", "valid_to"):
        document["event"].pop(field)
    raw = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    omitted = read_fact_envelope(raw)
    connection = _InertConnection()
    with pytest.raises(_TransactionBoundaryReachedError):
        EvidenceStore(connection).append(omitted)
    assert connection.transaction_calls == 1


@pytest.mark.parametrize(
    "variant_name",
    ["event_int_to_bool", "observation_int_to_bool", "observation_int_to_decimal"],
)
def test_append_rejects_python_equal_but_type_distinct_event_and_observation_dtos(
    variant_name: str,
) -> None:
    if EvidenceStore is None:
        pytest.skip("optional psycopg control extra is not installed")
    canonical = _typed_envelope(1)
    variants = {
        "event_int_to_bool": type(canonical)(
            replace(canonical.event, object={"value": True}),
            canonical.observation,
            canonical.evidence,
            canonical.canonical_bytes,
        ),
        "observation_int_to_bool": type(canonical)(
            canonical.event,
            replace(canonical.observation, payload={"value": True}),
            canonical.evidence,
            canonical.canonical_bytes,
        ),
        "observation_int_to_decimal": type(canonical)(
            canonical.event,
            replace(canonical.observation, payload={"value": Decimal("1")}),
            canonical.evidence,
            canonical.canonical_bytes,
        ),
    }
    forged = variants[variant_name]
    assert canonical == forged
    connection = _InertConnection()
    with pytest.raises(ContractError) as exc_info:
        EvidenceStore(connection).append(forged)
    assert exc_info.value.args == ("canonical envelope bytes do not match validated values",)
    assert connection.transaction_calls == 0


def test_explicit_different_event_uuid_remains_a_valid_append_candidate() -> None:
    if EvidenceStore is None:
        pytest.skip("optional psycopg control extra is not installed")
    canonical = _typed_envelope(1)
    second_event = replace(
        canonical.event,
        event_id=uuid.UUID("7dce3104-281c-45f6-9cc8-5a9987a54a41"),
    )
    second = read_fact_envelope(
        serialize_fact_envelope(second_event, canonical.observation, evidence=canonical.evidence)
    )
    assert second.observation.observation_id == canonical.observation.observation_id
    assert event_identity(second.event) != event_identity(canonical.event)
    connection = _InertConnection()
    with pytest.raises(_TransactionBoundaryReachedError):
        EvidenceStore(connection).append(second)
    assert connection.transaction_calls == 1
