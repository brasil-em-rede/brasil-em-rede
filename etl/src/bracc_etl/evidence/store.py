from __future__ import annotations

from typing import TYPE_CHECKING, Any

from bracc_etl.evidence.contracts import (
    ContractError,
    FactEvent,
    ObservationRevision,
    RevisionAction,
    RevisionChain,
    ValidatedFactEnvelope,
    apply_revision,
    event_identity,
    read_fact_envelope,
    serialize_fact_envelope,
)

if TYPE_CHECKING:
    import uuid


class EvidenceStore:
    """Append validated evidence envelopes using a caller-owned connection."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def append(self, envelope: ValidatedFactEnvelope) -> RevisionChain:
        if not isinstance(envelope, ValidatedFactEnvelope):
            raise ContractError("envelope must be a ValidatedFactEnvelope")
        payload = envelope.canonical_bytes
        validated = read_fact_envelope(payload)
        provided_values = serialize_fact_envelope(
            envelope.event,
            envelope.observation,
            evidence=envelope.evidence,
        )
        validated_values = serialize_fact_envelope(
            validated.event,
            validated.observation,
            evidence=validated.evidence,
        )
        if provided_values != validated_values or payload != validated.canonical_bytes:
            raise ContractError("canonical envelope bytes do not match validated values")
        event = validated.event
        observation = validated.observation
        with self.connection.transaction():
            self._bind_catalog(validated)
            event_key = event_identity(event)
            self.connection.execute(
                "INSERT INTO evidence.event_lock(event_identity) VALUES (%s) "
                "ON CONFLICT DO NOTHING",
                (event_key,),
            )
            self.connection.execute(
                "SELECT event_identity FROM evidence.event_lock WHERE event_identity=%s FOR UPDATE",
                (event_key,),
            )
            rows = self.connection.execute(
                "SELECT canonical_bytes FROM evidence.journal "
                "WHERE event_identity=%s ORDER BY revision_no",
                (event_key,),
            ).fetchall()
            all_envelopes = [read_fact_envelope(bytes(row[0])) for row in rows]
            if any(event_identity(item.event) != event_key for item in all_envelopes):
                raise ContractError("business event identity has conflicting durable event values")
            all_chains: dict[tuple[str, str | None, str], list[ValidatedFactEnvelope]] = {}
            for item in all_envelopes:
                key = (
                    item.observation.record_locator,
                    item.observation.source_record_key,
                    item.observation.dataset_contract_version,
                )
                all_chains.setdefault(key, []).append(item)
            chain_key = (
                observation.record_locator,
                observation.source_record_key,
                observation.dataset_contract_version,
            )
            group = all_chains.get(chain_key, [])
            replay = next(
                (
                    item
                    for item in group
                    if item.observation.observation_id == observation.observation_id
                ),
                None,
            )
            if replay is not None:
                if replay.observation != observation:
                    raise ContractError("replayed observation ID has conflicting content")
                if replay.canonical_bytes != validated.canonical_bytes:
                    raise ContractError("replayed observation ID has conflicting envelope content")
                return RevisionChain(tuple(item.observation for item in group))
            if any(item.observation.action is RevisionAction.RETRACT for item in group):
                raise ContractError("retracted record cannot accept another revision")
            chain = RevisionChain(tuple(item.observation for item in group))
            updated = apply_revision(chain, observation)
            if updated is chain:
                return chain
            if len(updated.revisions) != len(chain.revisions) + 1:
                raise ContractError("revision append did not extend the durable chain")
            self.connection.execute(
                "INSERT INTO evidence.journal "
                "(event_identity,source_id,dataset_id,record_locator,revision_no,observation_id,"
                "event_id,artifact_id,artifact_sha256,canonical_bytes) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    event_key,
                    observation.source_id,
                    observation.dataset_id,
                    observation.record_locator,
                    observation.revision_no,
                    observation.observation_id,
                    event.event_id,
                    observation.artifact_id,
                    observation.artifact_sha256,
                    payload,
                ),
            )
            return updated

    def history(
        self, source_id: str, dataset_id: str, record_locator: str
    ) -> tuple[RevisionChain, ...]:
        rows = self.connection.execute(
            "SELECT event_identity,canonical_bytes FROM evidence.journal "
            "WHERE source_id=%s AND dataset_id=%s AND record_locator=%s "
            "ORDER BY event_identity,revision_no",
            (source_id, dataset_id, record_locator),
        ).fetchall()
        groups: dict[str, list[ObservationRevision]] = {}
        for key, raw in rows:
            envelope = read_fact_envelope(bytes(raw))
            groups.setdefault(str(key), []).append(envelope.observation)
        return tuple(RevisionChain(tuple(items)) for items in groups.values())

    def event_history(self, event: FactEvent) -> RevisionChain:
        rows = self.connection.execute(
            "SELECT canonical_bytes FROM evidence.journal "
            "WHERE event_identity=%s ORDER BY revision_no",
            (event_identity(event),),
        ).fetchall()
        envelopes = [read_fact_envelope(bytes(row[0])) for row in rows]
        if any(event_identity(item.event) != event_identity(event) for item in envelopes):
            raise ContractError("business event identity has conflicting durable event values")
        return RevisionChain(tuple(item.observation for item in envelopes))

    def envelopes(self, event: FactEvent) -> tuple[ValidatedFactEnvelope, ...]:
        rows = self.connection.execute(
            "SELECT canonical_bytes FROM evidence.journal "
            "WHERE event_identity=%s ORDER BY revision_no",
            (event_identity(event),),
        ).fetchall()
        envelopes = tuple(read_fact_envelope(bytes(row[0])) for row in rows)
        if any(event_identity(item.event) != event_identity(event) for item in envelopes):
            raise ContractError("business event identity has conflicting durable event values")
        return envelopes

    def _bind_catalog(self, envelope: ValidatedFactEnvelope) -> None:
        observation = envelope.observation
        refs = [
            (
                observation.artifact_id,
                observation.artifact_sha256,
                observation.observation_id,
                observation.record_locator,
            )
        ]
        refs.extend(
            (item.artifact_id, item.artifact_sha256, item.observation_id, item.record_locator)
            for item in envelope.evidence
        )
        if any(
            item.observation_id != observation.observation_id
            or item.record_locator != observation.record_locator
            for item in envelope.evidence
        ):
            raise ContractError("evidence reference observation or record locator mismatch")
        for artifact_id, digest, observation_id, locator in refs:
            if (
                observation_id != observation.observation_id
                or locator != observation.record_locator
            ):
                raise ContractError("evidence reference observation or record locator mismatch")
            if (
                envelope.evidence
                and envelope.evidence[0].observation_id != observation.observation_id
            ):
                raise ContractError("evidence reference observation or record locator mismatch")
            row = self.connection.execute(
                "SELECT a.sha256,a.partition_id,d.id,d.contract_version,d.source_id,s.id "
                "FROM control.artifact a JOIN control.partition p ON p.id=a.partition_id "
                "JOIN control.dataset d ON d.id=p.dataset_id "
                "JOIN control.source s ON s.id=d.source_id WHERE a.id=%s",
                (artifact_id,),
            ).fetchone()
            if row is None or row[0] != digest:
                raise ContractError("evidence artifact catalog identity or hash mismatch")
            if (row[2], row[3], row[4], row[5]) != (
                observation.dataset_id,
                observation.dataset_contract_version,
                observation.source_id,
                observation.source_id,
            ):
                raise ContractError(
                    "evidence artifact catalog source, dataset, or contract mismatch"
                )


def _evidence_refs(value: Any) -> list[tuple[uuid.UUID, str]]:
    from bracc_etl.evidence.contracts import EvidenceRef

    found: list[tuple[uuid.UUID, str]] = []
    if isinstance(value, EvidenceRef):
        found.append((value.artifact_id, value.artifact_sha256))
    elif isinstance(value, dict):
        for item in value.values():
            found.extend(_evidence_refs(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.extend(_evidence_refs(item))
    return found
