from __future__ import annotations

import csv
import hashlib
import io
import json
import operator
import os
import re
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
else:
    Callable = Iterator = Mapping = Any


class ExtractionContractError(ValueError):
    """Raised when structured extraction cannot be proven valid and complete."""


@dataclass(frozen=True)
class PopulationSpec:
    population_id: str
    schema_id: str
    schema_version: str
    required_artifacts: tuple[tuple[str, str], ...]
    parser: Callable[[Any], Iterator[Any]]
    schema: tuple[str, ...] = ()
    required: bool = True


@dataclass(frozen=True)
class SourceAdapterSpec:
    adapter_id: str
    adapter_version: str
    source_id: str
    expected_source_url: str
    manifest_schema_version: int
    populations: tuple[PopulationSpec, ...]


@dataclass(frozen=True)
class ArtifactBinding:
    artifact_id: str
    population_id: str
    relative_path: str
    format: str
    sha256: str
    byte_length: int
    schema_id: str
    schema_version: str


@dataclass(frozen=True)
class ExtractionPopulation:
    population_id: str
    required: bool
    bindings: tuple[ArtifactBinding, ...]
    _cursors: tuple[Any, ...]


_RESULT_AUTHORITY = object()


@dataclass(frozen=True)
class ExtractionResult:
    run_id: str
    source_id: str
    source_url: str
    adapter_id: str
    adapter_version: str
    adapter_policy: tuple[tuple[str, bool], ...]
    manifest_sha256: str
    populations: tuple[ExtractionPopulation, ...]
    _library_authority: object | None = field(default=None, repr=False, compare=False)


class _HashingReader:
    def __init__(self, raw: Any) -> None:
        self._raw = raw
        self.digest = hashlib.sha256()
        self.byte_length = 0
        self.eof = False
        self.closed = False
        self.expected_header: tuple[str, ...] = ()
        self.cursor: Any = None

    def read(self, size: int = -1) -> bytes:
        data = self._raw.read(size)
        if not isinstance(data, bytes):
            raise ExtractionContractError("binary reader returned non-bytes")
        if data:
            self.digest.update(data)
            self.byte_length += len(data)
        else:
            self.eof = True
        return data

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._raw.close()


class _ReaderAdapter(io.RawIOBase):
    def __init__(self, reader: _HashingReader) -> None:
        super().__init__()
        self.reader = reader
        self.name = "<bound-extraction-stream>"

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        data = self.reader.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


class _OneShotCursor:
    def __init__(
        self, binding: ArtifactBinding, reader: _HashingReader, parser: Iterator[Any]
    ) -> None:
        self.binding = binding
        self._reader = reader
        self._parser = parser
        self.records_seen = 0
        self.state = "aberto"
        self._parser_eof = False
        self._schema_seen = False

    def __iter__(self) -> _OneShotCursor:
        return self

    def __next__(self) -> Any:
        if self.state != "aberto":
            raise ExtractionContractError("cursor is not open")
        try:
            record = next(self._parser)
        except StopIteration:
            self._parser_eof = True
            try:
                if self._reader.read(1) != b"":
                    raise ExtractionContractError("parser left unread artifact bytes")
                if not self._reader.eof:
                    raise ExtractionContractError("physical EOF not observed")
                if self._reader.digest.hexdigest() != self.binding.sha256:
                    raise ExtractionContractError("artifact digest mismatch")
                if self._reader.byte_length != self.binding.byte_length:
                    raise ExtractionContractError("artifact byte length mismatch")
                if not self._schema_seen:
                    raise ExtractionContractError("artifact schema was not observed")
                self.state = "verificado-completo"
            except Exception:
                self.state = "falha"
                raise
            raise
        except Exception as exc:
            self.state = "falha"
            if isinstance(exc, ExtractionContractError):
                raise
            raise ExtractionContractError("parser or stream failure") from exc
        self.records_seen += 1
        return record

    def mark_schema_seen(self) -> None:
        self._schema_seen = True

    def close(self) -> None:
        self._reader.close()
        if self.state not in {"verificado-completo", "falha"}:
            self.state = "fechado"


def csv_dict_parser(reader: _HashingReader) -> Iterator[dict[str, str]]:
    text = io.TextIOWrapper(
        io.BufferedReader(_ReaderAdapter(reader)), encoding="utf-8-sig", newline=""
    )
    rows = csv.reader(text, strict=True)
    header = next(rows, None)
    if header is None or len(set(header)) != len(header):
        raise ExtractionContractError("missing or duplicate CSV header")
    if tuple(header) != reader.expected_header:
        raise ExtractionContractError("CSV schema mismatch")
    reader.cursor.mark_schema_seen()
    for values in rows:
        if len(values) != len(header):
            raise ExtractionContractError("CSV row schema mismatch")
        yield dict(zip(header, values, strict=True))


def read_manifest(path: Path) -> tuple[dict[str, Any], bytes]:
    raw = path.read_bytes()
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExtractionContractError("invalid manifest JSON") from exc
    if not isinstance(obj, dict):
        raise ExtractionContractError("manifest must be an object")
    return obj, raw


def validate_manifest(
    manifest: Mapping[str, Any],
    adapter: SourceAdapterSpec,
    *,
    run_id: str,
    source_id: str,
    source_url: str,
) -> tuple[ArtifactBinding, ...]:
    fields = {
        "manifest_schema_version",
        "adapter_id",
        "adapter_version",
        "run_id",
        "source_id",
        "source_url",
        "populations",
    }
    if set(manifest) != fields:
        raise ExtractionContractError("manifest fields differ from ManifestV1")
    if (
        type(manifest["manifest_schema_version"]) is not int
        or manifest["manifest_schema_version"] != 1
        or type(adapter.manifest_schema_version) is not int
        or adapter.manifest_schema_version != 1
    ):
        raise ExtractionContractError("unsupported manifest version")
    identity_keys = ("adapter_id", "adapter_version", "run_id", "source_id", "source_url")
    if any(not isinstance(manifest[key], str) or not manifest[key] for key in identity_keys):
        raise ExtractionContractError("manifest identity fields must be non-empty strings")
    identity = (adapter.adapter_id, adapter.adapter_version, run_id, source_id, source_url)
    actual_identity = tuple(manifest[key] for key in identity_keys)
    if (
        actual_identity != identity
        or source_id != adapter.source_id
        or source_url != adapter.expected_source_url
    ):
        raise ExtractionContractError("manifest or adapter identity mismatch")
    populations = manifest["populations"]
    if not isinstance(populations, list):
        raise ExtractionContractError("populations must be a list")
    expected = {p.population_id: p for p in adapter.populations}
    ids: list[str] = []
    for population in populations:
        if not isinstance(population, dict):
            raise ExtractionContractError("invalid population entry")
        population_id = population.get("population_id")
        if not isinstance(population_id, str) or not population_id:
            raise ExtractionContractError("population_id must be a non-empty string")
        ids.append(population_id)
    if (
        len(expected) != len(adapter.populations)
        or len(ids) != len(populations)
        or len(ids) != len(set(ids))
        or set(ids) != set(expected)
    ):
        raise ExtractionContractError("population coverage mismatch")
    bindings: list[ArtifactBinding] = []
    for pop in populations:
        if not isinstance(pop, dict) or set(pop) != {"population_id", "artifacts"}:
            raise ExtractionContractError("invalid population entry")
        if not isinstance(pop["artifacts"], list):
            raise ExtractionContractError("invalid population entry")
        spec = expected[pop["population_id"]]
        actual: set[tuple[str, str]] = set()
        for item in pop["artifacts"]:
            artifact_fields = {
                "artifact_id",
                "relative_path",
                "format",
                "sha256",
                "byte_length",
                "schema_id",
                "schema_version",
            }
            if not isinstance(item, dict) or set(item) != artifact_fields:
                raise ExtractionContractError("invalid artifact binding")
            string_fields = ("artifact_id", "format", "schema_id", "schema_version")
            if any(not isinstance(item[field], str) or not item[field] for field in string_fields):
                raise ExtractionContractError("artifact identity fields must be non-empty strings")
            rel = item["relative_path"]
            if (
                not isinstance(rel, str)
                or not rel
                or "\x00" in rel
                or Path(rel).is_absolute()
                or ".." in Path(rel).parts
            ):
                raise ExtractionContractError("unsafe artifact path")
            try:
                os.fsencode(rel)
            except UnicodeEncodeError as exc:
                raise ExtractionContractError(
                    "artifact path is not representable in filesystem encoding"
                ) from exc
            digest, size = item["sha256"], item["byte_length"]
            if (
                not isinstance(digest, str)
                or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                or type(size) is not int
                or size < 0
            ):
                raise ExtractionContractError("invalid artifact digest or length")
            if (item["schema_id"], item["schema_version"]) != (spec.schema_id, spec.schema_version):
                raise ExtractionContractError("artifact schema identity mismatch")
            key = (item["artifact_id"], item["format"])
            if key in actual:
                raise ExtractionContractError("duplicate artifact")
            actual.add(key)
            bindings.append(
                ArtifactBinding(
                    item["artifact_id"],
                    pop["population_id"],
                    rel,
                    item["format"],
                    digest,
                    size,
                    item["schema_id"],
                    item["schema_version"],
                )
            )
        if actual != set(spec.required_artifacts):
            raise ExtractionContractError("artifact coverage mismatch")
    return tuple(bindings)


def _open_beneath_root(root: Path, relative_path: str) -> Any:
    """Open one regular file through a no-symlink dirfd walk under trusted root."""
    parts = Path(relative_path).parts
    if (
        not parts
        or Path(relative_path).is_absolute()
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise ExtractionContractError("unsafe artifact path")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    file_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
    root_fd: int | None = None
    directory_fds: list[int] = []
    artifact_fd: int | None = None
    try:
        root_fd = os.open(root, directory_flags)
        current_fd = root_fd
        for part in parts[:-1]:
            current_fd = os.open(part, directory_flags, dir_fd=current_fd)
            directory_fds.append(current_fd)
        artifact_fd = os.open(parts[-1], file_flags, dir_fd=current_fd)
        if not stat.S_ISREG(os.fstat(artifact_fd).st_mode):
            raise ExtractionContractError("artifact is not a regular file")
        stream = os.fdopen(artifact_fd, "rb")
        artifact_fd = None
        return stream
    except OSError as exc:
        raise ExtractionContractError(
            "artifact could not be opened securely beneath trusted root"
        ) from exc
    finally:
        if artifact_fd is not None:
            os.close(artifact_fd)
        for directory_fd in reversed(directory_fds):
            os.close(directory_fd)
        if root_fd is not None:
            os.close(root_fd)


def open_extraction(
    root: Path,
    manifest_path: Path,
    adapter: SourceAdapterSpec,
    *,
    run_id: str,
    source_id: str,
    source_url: str,
) -> tuple[ExtractionResult, tuple[_OneShotCursor, ...]]:
    manifest, raw_manifest = read_manifest(manifest_path)
    bindings = validate_manifest(
        manifest, adapter, run_id=run_id, source_id=source_id, source_url=source_url
    )
    specs = {p.population_id: p for p in adapter.populations}
    cursors: list[_OneShotCursor] = []
    grouped: dict[str, list[tuple[ArtifactBinding, _OneShotCursor]]] = {
        p.population_id: [] for p in adapter.populations
    }
    try:
        for binding in bindings:
            raw_artifact = _open_beneath_root(root, binding.relative_path)
            reader = _HashingReader(raw_artifact)
            spec = specs[binding.population_id]
            if binding.format == "csv" and spec.parser is csv_dict_parser:
                reader.expected_header = spec.schema
            cursor = _OneShotCursor(binding, reader, iter(()))
            reader.cursor = cursor
            cursors.append(cursor)
            cursor._parser = iter(spec.parser(reader))
            grouped[binding.population_id].append((binding, cursor))
        pops = tuple(
            ExtractionPopulation(
                p.population_id,
                p.required,
                tuple(b for b, _ in grouped[p.population_id]),
                tuple(c for _, c in grouped[p.population_id]),
            )
            for p in adapter.populations
        )
        result = ExtractionResult(
            run_id,
            source_id,
            source_url,
            adapter.adapter_id,
            adapter.adapter_version,
            tuple((p.population_id, p.required) for p in adapter.populations),
            hashlib.sha256(raw_manifest).hexdigest(),
            pops,
            _library_authority=_RESULT_AUTHORITY,
        )
        return result, tuple(cursors)
    except Exception:
        for cursor in cursors:
            cursor.close()
        raise


def verify_complete(cursors: tuple[_OneShotCursor, ...]) -> None:
    if not cursors or any(cursor.state != "verificado-completo" for cursor in cursors):
        raise ExtractionContractError("structured extraction incomplete")


@dataclass(frozen=True)
class InputPlan:
    result: ExtractionResult


@dataclass(frozen=True)
class FilterPlan:
    input: Any
    field: str
    operation: str
    value: Any


@dataclass(frozen=True)
class FieldExpr:
    source: str
    output: str | None = None


@dataclass(frozen=True)
class SelectPlan:
    input: Any
    fields: tuple[str | FieldExpr, ...]


@dataclass(frozen=True)
class GroupAggregatePlan:
    input: Any
    keys: tuple[str, ...]
    aggregates: tuple[tuple[str, str, str | None], ...]


TransformPlan = InputPlan | FilterPlan | SelectPlan | GroupAggregatePlan


@dataclass(frozen=True)
class _Tracked:
    values: dict[str, Any]
    members: frozenset[tuple[str, str, int]]


@dataclass(frozen=True)
class VerifiedOutput:
    result: ExtractionResult
    rows: tuple[dict[str, Any], ...]
    source_empty: bool
    output_empty_after_filter: bool


def execute_transform(
    result: ExtractionResult, cursors: tuple[Any, ...], plan: Any
) -> VerifiedOutput:
    if type(result) is not ExtractionResult or result._library_authority is not _RESULT_AUTHORITY:
        raise ExtractionContractError("extraction result was not issued by the library")

    def validate(node: Any) -> None:
        if type(node) is InputPlan:
            if node.result is not result:
                raise ExtractionContractError("transform plan result identity mismatch")
            return
        if type(node) is FilterPlan:
            validate(node.input)
            if type(node.field) is not str or not node.field:
                raise ExtractionContractError("invalid filter specification")
            if type(node.operation) is not str or node.operation not in {
                "eq",
                "ne",
                "lt",
                "le",
                "gt",
                "ge",
                "in",
                "not_in",
                "is_null",
                "not_null",
            }:
                raise ExtractionContractError("invalid filter specification")
            scalar = (str, int, float, bool, type(None))
            if node.operation in {"in", "not_in"}:
                if type(node.value) not in {tuple, frozenset} or any(
                    type(value) not in scalar for value in node.value
                ):
                    raise ExtractionContractError("invalid filter value")
            elif node.operation in {"is_null", "not_null"}:
                if node.value is not None:
                    raise ExtractionContractError("invalid filter value")
            elif type(node.value) not in scalar:
                raise ExtractionContractError("invalid filter value")
            return
        if type(node) is SelectPlan:
            validate(node.input)
            if not isinstance(node.fields, tuple) or not node.fields:
                raise ExtractionContractError("invalid select specification")
            outputs: set[str] = set()
            for expression in node.fields:
                if type(expression) is str:
                    source = output = expression
                elif type(expression) is FieldExpr:
                    source = expression.source
                    output = expression.output if expression.output is not None else source
                else:
                    raise ExtractionContractError("invalid select specification")
                if (
                    type(source) is not str
                    or not source
                    or type(output) is not str
                    or not output
                    or output in outputs
                ):
                    raise ExtractionContractError("invalid select specification")
                outputs.add(output)
            return
        if type(node) is GroupAggregatePlan:
            validate(node.input)
            if (
                not isinstance(node.keys, tuple)
                or not node.keys
                or any(not isinstance(key, str) or not key for key in node.keys)
                or len(set(node.keys)) != len(node.keys)
                or not isinstance(node.aggregates, tuple)
                or not node.aggregates
            ):
                raise ExtractionContractError("invalid aggregate specification")
            names: set[str] = set(node.keys)
            for aggregate in node.aggregates:
                if not isinstance(aggregate, tuple) or len(aggregate) != 3:
                    raise ExtractionContractError("invalid aggregate specification")
                name, operation, field = aggregate
                if (
                    not isinstance(name, str)
                    or not name
                    or name in names
                    or operation not in {"count", "sum", "min", "max"}
                    or (operation == "count" and field is not None)
                    or (operation != "count" and (not isinstance(field, str) or not field))
                ):
                    raise ExtractionContractError("invalid aggregate specification")
                names.add(name)
            return
        raise ExtractionContractError("structured transform must return a TransformPlan")

    validate(plan)
    owned = tuple(cursor for population in result.populations for cursor in population._cursors)
    if any(type(cursor) is not _OneShotCursor for cursor in owned):
        raise ExtractionContractError("extraction contains an unverified cursor type")
    if owned != cursors or len({id(cursor) for cursor in cursors}) != len(cursors):
        raise ExtractionContractError("cursor binding mismatch or sharing")

    policy = tuple(
        (population.population_id, population.required) for population in result.populations
    )
    if policy != result.adapter_policy or not any(required for _, required in policy):
        raise ExtractionContractError("producer population policy binding mismatch")
    positions = {
        id(cursor): (population.population_id, cursor.binding.artifact_id)
        for population in result.populations
        for cursor in population._cursors
    }

    records: list[_Tracked] = []
    for cursor in cursors:
        if cursor.state == "verificado-completo":
            raise ExtractionContractError("cursor was consumed before library execution")
        for index, row in enumerate(cursor):
            if not isinstance(row, dict):
                raise ExtractionContractError("extracted record must be a mapping")
            population_id, artifact_id = positions[id(cursor)]
            records.append(
                _Tracked(
                    dict(row),
                    frozenset({(population_id, artifact_id, index)}),
                )
            )
    verify_complete(cursors)

    required = {
        population.population_id for population in result.populations if population.required
    }
    represented_required = {
        next(iter(row.members))[0]
        for row in records
        if row.members and next(iter(row.members))[0] in required
    }
    empty_required = required - represented_required
    if empty_required and empty_required != required:
        raise ExtractionContractError("required population is empty")
    source_empty = empty_required == required
    if source_empty and records:
        raise ExtractionContractError("empty required population is hidden by optional records")

    def run(node: Any) -> list[_Tracked]:
        if isinstance(node, InputPlan):
            if node.result is not result:
                raise ExtractionContractError("transform plan result identity mismatch")
            return records
        if isinstance(node, FilterPlan):
            rows = run(node.input)
            operations: dict[str, Callable[[Any, Any], bool]] = {
                "eq": operator.eq,
                "ne": operator.ne,
                "lt": operator.lt,
                "le": operator.le,
                "gt": operator.gt,
                "ge": operator.ge,
                "in": lambda left, right: left in right,
                "not_in": lambda left, right: left not in right,
                "is_null": lambda left, right: left is None,
                "not_null": lambda left, right: left is not None,
            }
            if any(node.field not in row.values for row in rows):
                raise ExtractionContractError("filter field is missing")
            try:
                return [
                    row
                    for row in rows
                    if operations[node.operation](row.values[node.field], node.value)
                ]
            except (TypeError, ValueError) as exc:
                raise ExtractionContractError("invalid filter comparison") from exc
        if isinstance(node, SelectPlan):
            rows = run(node.input)
            try:
                selected_rows: list[_Tracked] = []
                for row in rows:
                    values: dict[str, Any] = {}
                    for expression in node.fields:
                        if isinstance(expression, FieldExpr):
                            source_field = expression.source
                            output_field = expression.output or source_field
                        else:
                            source_field = output_field = expression
                        values[output_field] = row.values[source_field]
                    selected_rows.append(_Tracked(values, row.members))
                return selected_rows
            except KeyError as exc:
                raise ExtractionContractError("selected field is missing") from exc
        if isinstance(node, GroupAggregatePlan):
            rows = run(node.input)
            if not node.keys or any(not isinstance(key, str) for key in node.keys):
                raise ExtractionContractError("invalid group keys")
            groups: dict[tuple[Any, ...], list[_Tracked]] = {}
            try:
                for row in rows:
                    groups.setdefault(tuple(row.values[key] for key in node.keys), []).append(row)
            except KeyError as exc:
                raise ExtractionContractError("group field is missing") from exc
            output: list[_Tracked] = []
            for key, contributors in groups.items():
                values = dict(zip(node.keys, key, strict=True))
                for name, operation, field in node.aggregates:
                    try:
                        aggregate_values = (
                            [row.values[field] for row in contributors] if field is not None else []
                        )
                        if operation == "count":
                            value = len(contributors)
                        elif operation == "sum":
                            value = sum(aggregate_values)
                        elif operation == "min":
                            value = min(aggregate_values)
                        elif operation == "max":
                            value = max(aggregate_values)
                        else:
                            raise ExtractionContractError("invalid aggregate specification")
                    except (KeyError, TypeError, ValueError) as exc:
                        raise ExtractionContractError("invalid aggregate inputs") from exc
                    if name in values or not isinstance(name, str):
                        raise ExtractionContractError("invalid aggregate output field")
                    values[name] = value
                members = frozenset().union(*(row.members for row in contributors))
                if not members:
                    raise ExtractionContractError("aggregate group has no source members")
                output.append(_Tracked(values, members))
            return output
        raise ExtractionContractError("structured transform must return a TransformPlan")

    rows = run(plan)
    if source_empty and rows:
        raise ExtractionContractError("verified empty source cannot produce rows")
    return VerifiedOutput(
        result,
        tuple(dict(row.values) for row in rows),
        source_empty,
        bool(records) and not rows,
    )
