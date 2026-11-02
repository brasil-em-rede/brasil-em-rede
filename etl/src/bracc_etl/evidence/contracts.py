"""Pure immutable contracts for facts, evidence and append-only revisions."""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import Any

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_EVENT_DOMAIN = "br-acc.fact-event.v2"
_OBSERVATION_DOMAIN = "br-acc.observation.v1"
_WIRE_FORMAT = "br-acc-typed-json/v1"


class ContractError(ValueError):
    """A fact or observation violates its explicit value contract."""


class IdentityUnresolvedError(ContractError):
    """No explicit stable producer business-event UUID was supplied."""


class ReferenceStatus(StrEnum):
    UNRESOLVED = "unresolved"
    EXACT_REFERENCE = "exact_reference"
    CANDIDATE_REFERENCE = "candidate_reference"


class RevisionAction(StrEnum):
    ASSERT = "assert"
    CORRECT = "correct"
    RETRACT = "retract"


def _required_text(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{name} must be a non-empty string")
    return value


def _uuid(name: str, value: object, *, optional: bool = False) -> uuid.UUID | None:
    if value is None and optional:
        return None
    if isinstance(value, uuid.UUID):
        return value
    if isinstance(value, str):
        try:
            return uuid.UUID(value)
        except ValueError as exc:
            raise ContractError(f"{name} must be a UUID") from exc
    raise ContractError(f"{name} must be a UUID")


def _aware_datetime(name: str, value: datetime | None) -> datetime | None:
    if value is not None and (not isinstance(value, datetime) or value.utcoffset() is None):
        raise ContractError(f"{name} must be a timezone-aware datetime")
    return value


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    return value


def _json_value(value: Any, path: str = "payload") -> Any:
    """Encode values as a closed tagged algebra; ordinary containers cannot mimic tags."""
    if value is None:
        return ["null", None]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ContractError(f"{path} contains a non-finite number")
        return ["float", repr(value)]
    if isinstance(value, str):
        return ["string", value]
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ContractError(f"{path} contains a non-finite decimal")
        return ["decimal", str(value)]
    if isinstance(value, uuid.UUID):
        return ["uuid", str(value)]
    if isinstance(value, datetime):
        if value.utcoffset() is None:
            raise ContractError(f"{path} contains a naive datetime")
        return ["datetime", value.isoformat()]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ContractError(f"{path} object keys must be strings")
        entries = [[key, _json_value(value[key], f"{path}.{key}")] for key in sorted(value)]
        return ["object", entries]
    if isinstance(value, (list, tuple)):
        items = [_json_value(item, f"{path}[{index}]") for index, item in enumerate(value)]
        return ["array", items]
    raise ContractError(f"{path} contains unsupported type {type(value).__name__}")


def _decode_value(value: Any, path: str = "payload") -> Any:
    if not isinstance(value, list) or len(value) != 2 or not isinstance(value[0], str):
        raise ContractError(f"{path} is not a tagged wire value")
    tag, item = value
    try:
        if tag == "null" and item is None:
            return None
        if tag == "bool" and isinstance(item, bool):
            return item
        if tag == "int" and isinstance(item, int) and not isinstance(item, bool):
            return item
        if tag == "float" and isinstance(item, str):
            result = float(item)
            if math.isfinite(result) and repr(result) == item:
                return result
        if tag == "string" and isinstance(item, str):
            return item
        if tag == "decimal" and isinstance(item, str):
            result_decimal = Decimal(item)
            if result_decimal.is_finite() and str(result_decimal) == item:
                return result_decimal
        if tag == "uuid" and isinstance(item, str):
            result_uuid = uuid.UUID(item)
            if str(result_uuid) == item:
                return result_uuid
        if tag == "datetime" and isinstance(item, str):
            result_datetime = datetime.fromisoformat(item)
            if result_datetime.utcoffset() is not None and result_datetime.isoformat() == item:
                return result_datetime
        if tag == "date" and isinstance(item, str):
            result_date = date.fromisoformat(item)
            if result_date.isoformat() == item:
                return result_date
        if tag == "array" and isinstance(item, list):
            return [_decode_value(nested, f"{path}[{index}]") for index, nested in enumerate(item)]
        if tag == "object" and isinstance(item, list):
            pairs: list[tuple[str, Any]] = []
            for index, pair in enumerate(item):
                if (not isinstance(pair, list) or len(pair) != 2 or not isinstance(pair[0], str)):
                    raise ContractError(f"{path} object entry {index} is invalid")
                pairs.append((pair[0], _decode_value(pair[1], f"{path}.{pair[0]}")))
            if [key for key, _ in pairs] != sorted({key for key, _ in pairs}):
                raise ContractError(f"{path} object keys must be unique and sorted")
            return dict(pairs)
    except (ValueError, ArithmeticError) as exc:
        raise ContractError(f"{path} has an invalid {tag!r} wire value") from exc
    raise ContractError(f"{path} has an invalid {tag!r} wire value")


def canonical_json(value: Any) -> str:
    """Return deterministic typed-wire JSON v1, rejecting values outside its algebra."""
    try:
        return json.dumps(
            [_WIRE_FORMAT, _json_value(value)],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        if isinstance(exc, ContractError):
            raise
        raise ContractError("value cannot be represented as canonical JSON") from exc


def decode_canonical_json(value: str) -> Any:
    """Decode typed-wire JSON v1 without interpreting user containers as tags."""
    try:
        document = json.loads(value)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ContractError("value is not valid typed-wire JSON") from exc
    if not isinstance(document, list) or len(document) != 2 or document[0] != _WIRE_FORMAT:
        raise ContractError("unsupported typed-wire JSON format version")
    return _decode_value(document[1])


def _digest_text(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class EntityRef:
    entity_type: str
    entity_id: uuid.UUID | None = None
    namespace: str | None = None
    scope: str | None = None
    status: ReferenceStatus = ReferenceStatus.UNRESOLVED

    def __post_init__(self) -> None:
        _required_text("entity_type", self.entity_type)
        object.__setattr__(self, "entity_id", _uuid("entity_id", self.entity_id, optional=True))
        if not isinstance(self.status, ReferenceStatus):
            try:
                object.__setattr__(self, "status", ReferenceStatus(self.status))
            except ValueError as exc:
                raise ContractError("unsupported reference status") from exc
        for key in ("namespace", "scope"):
            value = getattr(self, key)
            if value is not None:
                _required_text(key, value)
        if self.status is ReferenceStatus.UNRESOLVED and self.entity_id is not None:
            raise ContractError("unresolved reference cannot carry entity_id")
        if self.status is not ReferenceStatus.UNRESOLVED and self.entity_id is None:
            raise ContractError("resolved reference status requires entity_id")


@dataclass(frozen=True, slots=True)
class FactEvent:
    event_id: uuid.UUID
    source_id: str
    dataset_id: str
    dataset_contract_version: str
    subject: EntityRef
    predicate: str
    object: Any
    event_at: date | datetime | None = None
    valid_from: date | datetime | None = None
    valid_to: date | datetime | None = None
    date_precision: str = "unknown"
    source_record_key: str | None = None

    def __post_init__(self) -> None:
        if self.event_id is None:
            raise IdentityUnresolvedError(
                "business event identity requires an explicit producer event_id"
            )
        object.__setattr__(self, "event_id", _uuid("event_id", self.event_id))
        for name in ("source_id", "dataset_id", "dataset_contract_version", "predicate"):
            _required_text(name, getattr(self, name))
        if not isinstance(self.subject, EntityRef):
            raise ContractError("subject must be an EntityRef")
        if isinstance(self.object, EntityRef):
            pass
        else:
            canonical_json(self.object)
        for name in ("event_at", "valid_from", "valid_to"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, (date, datetime)):
                raise ContractError(f"{name} must be a date, datetime, or null")
            if isinstance(value, datetime):
                _aware_datetime(name, value)
        temporal_values = [self.event_at, self.valid_from, self.valid_to]
        kinds = {
            datetime if isinstance(value, datetime) else date
            for value in temporal_values
            if value is not None
        }
        if len(kinds) > 1:
            raise ContractError("temporal values must use the same temporal kind")
        if (
            self.valid_from is not None
            and self.valid_to is not None
            and self.valid_to < self.valid_from
        ):
            raise ContractError("valid_to must not precede valid_from")
        if self.date_precision not in {"day", "instant", "unknown"}:
            raise ContractError("unsupported date_precision")
        temporal_kind = next(iter(kinds), None)
        if self.date_precision == "unknown" and temporal_kind is not None:
            raise ContractError("unknown date precision requires all temporal values to be null")
        if self.date_precision == "day" and temporal_kind not in {None, date}:
            raise ContractError("day precision requires date values")
        if self.date_precision == "instant" and temporal_kind not in {None, datetime}:
            raise ContractError("instant precision requires datetime values")
        if self.source_record_key is not None:
            _required_text("source_record_key", self.source_record_key)
        object.__setattr__(self, "object", _freeze(self.object))


def event_identity(event: FactEvent) -> str:
    """Identify a producer event within its stable source/dataset namespace."""
    return _digest_text(
        {
            "domain": _EVENT_DOMAIN,
            "source_id": event.source_id,
            "dataset_id": event.dataset_id,
            "event_id": str(event.event_id),
        }
    )


@dataclass(frozen=True, slots=True)
class ObservationRevision:
    source_id: str
    dataset_id: str
    dataset_contract_version: str
    artifact_id: uuid.UUID
    artifact_sha256: str
    record_locator: str
    locator_kind: str
    payload_schema_version: str
    payload: Any
    revision_no: int = 1
    action: RevisionAction = RevisionAction.ASSERT
    source_record_key: str | None = None
    source_release: str | None = None
    parser_version: str | None = None
    normalizer_version: str | None = None
    observed_at: datetime | None = None
    supersedes_observation_id: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "source_id",
            "dataset_id",
            "dataset_contract_version",
            "record_locator",
            "locator_kind",
            "payload_schema_version",
        ):
            _required_text(name, getattr(self, name))
        object.__setattr__(self, "artifact_id", _uuid("artifact_id", self.artifact_id))
        if not isinstance(self.artifact_sha256, str) or not _SHA256.fullmatch(self.artifact_sha256):
            raise ContractError("artifact_sha256 must be lowercase SHA-256 hex")
        for name in ("source_record_key", "source_release", "parser_version", "normalizer_version"):
            value = getattr(self, name)
            if value is not None:
                _required_text(name, value)
        if (
            not isinstance(self.revision_no, int)
            or isinstance(self.revision_no, bool)
            or self.revision_no < 1
        ):
            raise ContractError("revision_no must be an integer >= 1")
        if not isinstance(self.action, RevisionAction):
            try:
                object.__setattr__(self, "action", RevisionAction(self.action))
            except ValueError as exc:
                raise ContractError("unsupported revision action") from exc
        _aware_datetime("observed_at", self.observed_at)
        if self.supersedes_observation_id is not None and (
            not isinstance(self.supersedes_observation_id, str)
            or not _SHA256.fullmatch(self.supersedes_observation_id)
        ):
            raise ContractError("supersedes_observation_id must be lowercase SHA-256 hex")
        canonical_json(self.payload)
        object.__setattr__(self, "payload", _freeze(self.payload))
        if self.action is RevisionAction.ASSERT:
            if self.revision_no != 1 or self.supersedes_observation_id is not None:
                raise ContractError("assert must start a revision chain")
        elif self.revision_no < 2 or self.supersedes_observation_id is None:
            raise ContractError("correct/retract require a predecessor and revision_no >= 2")
        if self.action is RevisionAction.RETRACT and self.payload is not None:
            raise ContractError("retract must not carry replacement payload")
        if self.action is RevisionAction.CORRECT and self.payload is None:
            raise ContractError("correct requires replacement payload")

    @property
    def observation_id(self) -> str:
        return _digest_text(
            {
                "domain": _OBSERVATION_DOMAIN,
                "source_id": self.source_id,
                "dataset_id": self.dataset_id,
                "dataset_contract_version": self.dataset_contract_version,
                "source_record_key": self.source_record_key,
                "record_locator": self.record_locator,
                "artifact_sha256": self.artifact_sha256,
                "payload_schema_version": self.payload_schema_version,
                "payload_sha256": _digest_text(self.payload),
                "revision_no": self.revision_no,
                "action": self.action.value,
                "supersedes_observation_id": self.supersedes_observation_id,
            }
        )


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    observation_id: str
    artifact_id: uuid.UUID
    artifact_sha256: str
    record_locator: str
    locator_kind: str
    field_path: str | None = None

    def __post_init__(self) -> None:
        if not _SHA256.fullmatch(self.observation_id):
            raise ContractError("observation_id must be lowercase SHA-256 hex")
        object.__setattr__(self, "artifact_id", _uuid("artifact_id", self.artifact_id))
        if not _SHA256.fullmatch(self.artifact_sha256):
            raise ContractError("artifact_sha256 must be lowercase SHA-256 hex")
        _required_text("record_locator", self.record_locator)
        _required_text("locator_kind", self.locator_kind)
        if self.field_path is not None:
            _required_text("field_path", self.field_path)


@dataclass(frozen=True, slots=True)
class RevisionChain:
    """An append-only, validated chain for one source-namespaced record."""

    revisions: tuple[ObservationRevision, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.revisions, tuple):
            raise ContractError("revisions must be an immutable tuple")
        previous: ObservationRevision | None = None
        seen: set[str] = set()
        for item in self.revisions:
            if not isinstance(item, ObservationRevision):
                raise ContractError("revision chain contains an invalid item")
            if item.observation_id in seen:
                if item != previous:
                    raise ContractError("replayed observation ID has conflicting content")
                continue  # exact replay is idempotent
            if previous is None:
                if item.revision_no != 1 or item.action is not RevisionAction.ASSERT:
                    raise ContractError("revision chain must begin with assert")
            else:
                same_record = (
                    item.source_id,
                    item.dataset_id,
                    item.dataset_contract_version,
                    item.source_record_key,
                    item.record_locator,
                ) == (
                    previous.source_id,
                    previous.dataset_id,
                    previous.dataset_contract_version,
                    previous.source_record_key,
                    previous.record_locator,
                )
                if not same_record:
                    raise ContractError("revision chain cannot change record identity")
                if (
                    item.revision_no != previous.revision_no + 1
                    or item.supersedes_observation_id != previous.observation_id
                ):
                    raise ContractError("revision must supersede the exact prior observation")
                if previous.action is RevisionAction.RETRACT:
                    raise ContractError("retracted record cannot accept another revision")
            seen.add(item.observation_id)
            previous = item

    def append(self, revision: ObservationRevision) -> RevisionChain:
        for item in self.revisions:
            if item.observation_id == revision.observation_id:
                if item != revision:
                    raise ContractError("replayed observation ID has conflicting content")
                return self
        return RevisionChain(self.revisions + (revision,))


def apply_revision(chain: RevisionChain, revision: ObservationRevision) -> RevisionChain:
    """Apply one explicit observation; absence is represented by None and makes no change."""
    if revision is None:
        return chain
    if not isinstance(chain, RevisionChain):
        raise ContractError("chain must be a RevisionChain")
    if not isinstance(revision, ObservationRevision):
        raise ContractError("revision must be an ObservationRevision")
    return chain.append(revision)


@dataclass(frozen=True, slots=True)
class ValidatedFactEnvelope:
    """Immutable output of the public raw-JSON validation boundary."""

    event: FactEvent
    observation: ObservationRevision
    evidence: tuple[EvidenceRef, ...]
    canonical_bytes: bytes


def _reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError(f"duplicate JSON object member: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> Any:
    raise ContractError(f"non-JSON numeric constant is forbidden: {value}")


def _closed_object(value: Any, required: set[str], optional: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{name} must be an object")
    keys = set(value)
    if not required <= keys or keys - required - optional:
        raise ContractError(f"{name} has missing or unknown fields")
    return value


def _wire_document(value: Any, name: str) -> Any:
    if not isinstance(value, list) or len(value) != 2 or value[0] != _WIRE_FORMAT:
        raise ContractError(f"{name} must be a br-acc-typed-json/v1 document")
    return _decode_value(value[1], name)


def _envelope_datetime(value: Any, name: str) -> date | datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContractError(f"{name} must be a date, aware datetime, or null")
    try:
        if len(value) == 10:
            parsed_date = date.fromisoformat(value)
            if parsed_date.isoformat() == value:
                return parsed_date
        parsed_datetime = datetime.fromisoformat(value)
        if parsed_datetime.utcoffset() is not None and parsed_datetime.isoformat() == value:
            return parsed_datetime
    except (ValueError, TypeError, OverflowError) as exc:
        raise ContractError(f"{name} has an invalid temporal value") from exc
    raise ContractError(f"{name} has a noncanonical or timezone-naive temporal value")


def _entity_ref(value: Any, name: str) -> EntityRef:
    fields = _closed_object(
        value,
        {"entity_type", "status"},
        {"entity_id", "namespace", "scope"},
        name,
    )
    return EntityRef(
        entity_type=fields["entity_type"],
        entity_id=fields.get("entity_id"),
        namespace=fields.get("namespace"),
        scope=fields.get("scope"),
        status=fields["status"],
    )


def _entity_ref_document(value: EntityRef) -> dict[str, Any]:
    document: dict[str, Any] = {
        "entity_type": value.entity_type,
        "status": value.status.value,
    }
    if value.entity_id is not None:
        document["entity_id"] = str(value.entity_id)
    if value.namespace is not None:
        document["namespace"] = value.namespace
    if value.scope is not None:
        document["scope"] = value.scope
    return document


def _validate_envelope_namespace(event: FactEvent, observation: ObservationRevision) -> None:
    if (
        event.source_id != observation.source_id
        or event.dataset_id != observation.dataset_id
        or event.dataset_contract_version != observation.dataset_contract_version
    ):
        raise ContractError("event and observation namespace must match")


def serialize_fact_envelope(
    event: FactEvent,
    observation: ObservationRevision,
    evidence: tuple[EvidenceRef, ...] = (),
) -> bytes:
    """Serialize typed fact DTOs as deterministic UTF-8 public-envelope JSON."""
    if not isinstance(event, FactEvent):
        raise ContractError("event must be a FactEvent")
    if not isinstance(observation, ObservationRevision):
        raise ContractError("observation must be an ObservationRevision")
    if not isinstance(evidence, tuple) or any(
        not isinstance(item, EvidenceRef) for item in evidence
    ):
        raise ContractError("evidence must be a tuple of EvidenceRef values")
    _validate_envelope_namespace(event, observation)

    event_object: dict[str, Any] = {
        "event_id": str(event.event_id),
        "source_id": event.source_id,
        "dataset_id": event.dataset_id,
        "dataset_contract_version": event.dataset_contract_version,
        "subject": _entity_ref_document(event.subject),
        "predicate": event.predicate,
        "object": (
            _entity_ref_document(event.object)
            if isinstance(event.object, EntityRef)
            else json.loads(canonical_json(event.object))
        ),
        "date_precision": event.date_precision,
    }
    if event.source_record_key is not None:
        event_object["source_record_key"] = event.source_record_key
    for name in ("event_at", "valid_from", "valid_to"):
        value = getattr(event, name)
        event_object[name] = value.isoformat() if value is not None else None

    observation_object: dict[str, Any] = {
        "source_id": observation.source_id,
        "dataset_id": observation.dataset_id,
        "dataset_contract_version": observation.dataset_contract_version,
        "artifact_id": str(observation.artifact_id),
        "artifact_sha256": observation.artifact_sha256,
        "record_locator": observation.record_locator,
        "locator_kind": observation.locator_kind,
        "payload_schema_version": observation.payload_schema_version,
        "payload": json.loads(canonical_json(observation.payload)),
        "revision_no": observation.revision_no,
        "action": observation.action.value,
    }
    for name in ("source_record_key", "source_release", "parser_version", "normalizer_version"):
        value = getattr(observation, name)
        if value is not None:
            observation_object[name] = value
    if observation.observed_at is not None:
        observation_object["observed_at"] = observation.observed_at.isoformat()
    if observation.supersedes_observation_id is not None:
        observation_object["supersedes_observation_id"] = observation.supersedes_observation_id

    envelope: dict[str, Any] = {
        "schema_version": "1",
        "event": event_object,
        "observation": observation_object,
    }
    if evidence:
        envelope["evidence"] = [
            {
                "observation_id": item.observation_id,
                "artifact_id": str(item.artifact_id),
                "artifact_sha256": item.artifact_sha256,
                "record_locator": item.record_locator,
                "locator_kind": item.locator_kind,
                **({"field_path": item.field_path} if item.field_path is not None else {}),
            }
            for item in evidence
        ]
    try:
        return json.dumps(
            envelope,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        if isinstance(exc, ContractError):
            raise
        raise ContractError("fact envelope cannot be serialized") from exc


def read_fact_envelope(raw: bytes | str) -> ValidatedFactEnvelope:
    """Strictly parse, validate and canonicalize a raw JSON fact envelope.

    Draft 2020-12 describes the structural/lexical wire shape. This dependency-free
    runtime path applies the same closed shape checks, then typed decoder and DTO
    semantic invariants that JSON Schema cannot express (including cross-field time
    ordering and canonical tagged values). No caller-supplied validator is trusted.
    """
    try:
        if isinstance(raw, bytes):
            text = raw.decode("utf-8", errors="strict")
        elif isinstance(raw, str):
            text = raw
        else:
            raise ContractError("raw envelope must be UTF-8 bytes or text")
        document = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_json_constant,
        )
        root = _closed_object(
            document,
            {"schema_version", "event", "observation"},
            {"evidence"},
            "envelope",
        )
        if root["schema_version"] != "1":
            raise ContractError("unsupported fact envelope schema_version")

        if not isinstance(root["event"], dict):
            raise ContractError("event must be an object")
        if root["event"].get("event_id") is None:
            raise IdentityUnresolvedError(
                "business event identity requires an explicit producer event_id"
            )
        event_data = _closed_object(
            root["event"],
            {
                "event_id",
                "source_id",
                "dataset_id",
                "dataset_contract_version",
                "subject",
                "predicate",
                "object",
                "date_precision",
            },
            {"source_record_key", "event_at", "valid_from", "valid_to"},
            "event",
        )
        event_id = _uuid("event_id", event_data["event_id"])
        if event_id is None:
            raise ContractError("event_id must be a UUID")
        subject = _entity_ref(event_data["subject"], "event.subject")
        raw_object = event_data["object"]
        if isinstance(raw_object, list):
            object_value = _wire_document(raw_object, "event.object")
        else:
            object_value = _entity_ref(raw_object, "event.object")
        event = FactEvent(
            event_id=event_id,
            source_id=event_data["source_id"],
            dataset_id=event_data["dataset_id"],
            dataset_contract_version=event_data["dataset_contract_version"],
            subject=subject,
            predicate=event_data["predicate"],
            object=object_value,
            event_at=_envelope_datetime(event_data.get("event_at"), "event.event_at"),
            valid_from=_envelope_datetime(event_data.get("valid_from"), "event.valid_from"),
            valid_to=_envelope_datetime(event_data.get("valid_to"), "event.valid_to"),
            date_precision=event_data["date_precision"],
            source_record_key=event_data.get("source_record_key"),
        )

        observation_data = _closed_object(
            root["observation"],
            {
                "source_id",
                "dataset_id",
                "dataset_contract_version",
                "artifact_id",
                "artifact_sha256",
                "record_locator",
                "locator_kind",
                "payload_schema_version",
                "payload",
                "revision_no",
                "action",
            },
            {
                "source_record_key",
                "source_release",
                "parser_version",
                "normalizer_version",
                "observed_at",
                "supersedes_observation_id",
            },
            "observation",
        )
        payload = _wire_document(observation_data["payload"], "observation.payload")
        artifact_id = _uuid("artifact_id", observation_data["artifact_id"])
        if artifact_id is None:
            raise ContractError("artifact_id must be a UUID")
        observed_at = _envelope_datetime(
            observation_data.get("observed_at"), "observation.observed_at"
        )
        if observed_at is not None and not isinstance(observed_at, datetime):
            raise ContractError("observed_at must be an aware datetime")
        observation = ObservationRevision(
            source_id=observation_data["source_id"],
            dataset_id=observation_data["dataset_id"],
            dataset_contract_version=observation_data["dataset_contract_version"],
            artifact_id=artifact_id,
            artifact_sha256=observation_data["artifact_sha256"],
            record_locator=observation_data["record_locator"],
            locator_kind=observation_data["locator_kind"],
            payload_schema_version=observation_data["payload_schema_version"],
            payload=payload,
            revision_no=observation_data["revision_no"],
            action=observation_data["action"],
            source_record_key=observation_data.get("source_record_key"),
            source_release=observation_data.get("source_release"),
            parser_version=observation_data.get("parser_version"),
            normalizer_version=observation_data.get("normalizer_version"),
            observed_at=observed_at,
            supersedes_observation_id=observation_data.get("supersedes_observation_id"),
        )
        _validate_envelope_namespace(event, observation)
        evidence_values = root.get("evidence", [])
        if not isinstance(evidence_values, list):
            raise ContractError("evidence must be an array")
        evidence: list[EvidenceRef] = []
        for index, item in enumerate(evidence_values):
            fields = _closed_object(
                item,
                {
                    "observation_id",
                    "artifact_id",
                    "artifact_sha256",
                    "record_locator",
                    "locator_kind",
                },
                {"field_path"},
                f"evidence[{index}]",
            )
            evidence.append(EvidenceRef(**fields))
        canonical_bytes = json.dumps(
            document, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        return ValidatedFactEnvelope(event, observation, tuple(evidence), canonical_bytes)
    except ContractError:
        raise
    except (
        json.JSONDecodeError,
        UnicodeDecodeError,
        TypeError,
        ValueError,
        KeyError,
        OverflowError,
    ) as exc:
        raise ContractError("raw fact envelope violates the public contract") from exc


def apply_serialized_revision(chain: RevisionChain, raw: bytes | str) -> RevisionChain:
    """Validate raw envelope fully before applying its observation to the real chain."""
    validated = read_fact_envelope(raw)
    return apply_revision(chain, validated.observation)
