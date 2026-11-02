import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from bracc_etl.base import Pipeline
from bracc_etl.extraction import (
    ExtractionContractError,
    FilterPlan,
    InputPlan,
    PopulationSpec,
    SourceAdapterSpec,
    csv_dict_parser,
)

HEADER = ("Firm Name", "Country", "From Date", "To Date", "Grounds")
URL = "https://fixture.invalid/debarred.csv"


def fixture_adapter() -> SourceAdapterSpec:
    return SourceAdapterSpec(
        "world-bank-fixture",
        "1",
        "world_bank",
        URL,
        1,
        (
            PopulationSpec(
                "firms",
                "world_bank.debarred.fixture",
                "1",
                (("debarred", "csv"),),
                csv_dict_parser,
                HEADER,
            ),
        ),
    )


class StructuredPipeline(Pipeline):
    name = "world_bank"
    source_id = "world_bank"

    def __init__(self, driver: Any, root: Path, manifest_path: Path, *, discard: bool = False):
        super().__init__(driver, data_dir=str(root))
        self.root, self.manifest_path = root, manifest_path
        self.configured_source_url = URL
        self.extraction_adapter = fixture_adapter()
        self.discard = discard
        self.load_calls = 0
        self.result_seen = None
        self.produced_result = None
        self.records: list[dict[str, str]] = []
        self.record_count = 0

    def extract(self) -> None:
        pass

    def transform(self) -> None:
        raise AssertionError("structured path must use transform_result")

    def transform_result(self, result: Any) -> tuple[dict[str, Any], ...]:
        self.result_seen = result
        plan = InputPlan(result)
        if self.discard:
            return FilterPlan(plan, HEADER[0], "eq", "absent")
        return plan

    def load_extraction(self, records: Sequence[dict[str, Any]]) -> None:
        self.loaded_records = records
        self.load_calls += 1

    def load(self) -> None:
        self.load_calls += 1

    def produce_extraction(self) -> Any:
        from bracc_etl.extraction import open_extraction

        return open_extraction(
            self.root,
            self.manifest_path,
            self.extraction_adapter,
            run_id=self.run_id,
            source_id=self.source_identity.source_id,
            source_url=URL,
        )


def make_fixture(tmp_path: Path, content: bytes, *, relative_path: str = "debarred.csv") -> Path:
    artifact_path = tmp_path / relative_path
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(content)
    binding = {
        "artifact_id": "debarred",
        "relative_path": relative_path,
        "format": "csv",
        "sha256": hashlib.sha256(content).hexdigest(),
        "byte_length": len(content),
        "schema_id": "world_bank.debarred.fixture",
        "schema_version": "1",
    }
    manifest = {
        "manifest_schema_version": 1,
        "adapter_id": "world-bank-fixture",
        "adapter_version": "1",
        "run_id": "placeholder",
        "source_id": "world_bank",
        "source_url": URL,
        "populations": [{"population_id": "firms", "artifacts": [binding]}],
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def set_run_id_before_producer(pipeline: StructuredPipeline, manifest: Path) -> None:
    producer = pipeline.produce_extraction

    def bound_producer() -> Any:
        data = json.loads(manifest.read_text(encoding="utf-8"))
        data["run_id"] = pipeline.run_id
        manifest.write_text(json.dumps(data), encoding="utf-8")
        result = producer()
        pipeline.produced_result = result[0]
        return result

    pipeline.produce_extraction = bound_producer  # type: ignore[method-assign]


def test_pipeline_run_produces_and_passes_same_result_with_verified_fixture_zero(
    tmp_path: Path,
) -> None:
    content = (",".join(HEADER) + "\n").encode()
    manifest = make_fixture(tmp_path, content)
    pipeline = StructuredPipeline(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)
    pipeline.run()
    assert pipeline.result_seen is pipeline.produced_result
    assert pipeline.record_count == 0
    assert pipeline.load_calls == 1
    statuses = [
        call.args[1]["status"]
        for call in pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    ]
    assert statuses == ["running", "loaded"]


def test_pipeline_loads_verified_nonempty_transformed_records(tmp_path: Path) -> None:
    content = (",".join(HEADER) + "\n" + '"Retained","X","","",""\n').encode()
    manifest = make_fixture(tmp_path, content)
    pipeline = StructuredPipeline(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)

    pipeline.run()

    assert pipeline.loaded_records[0][HEADER[0]] == "Retained"
    assert pipeline.loaded_records == (
        {"Firm Name": "Retained", "Country": "X", "From Date": "", "To Date": "", "Grounds": ""},
    )
    assert pipeline.load_calls == 1
    statuses = [
        call.args[1]["status"]
        for call in pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    ]
    assert statuses == ["running", "loaded"]


def test_parser_and_digest_share_one_physical_binary_open_with_buffering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    content = (",".join(HEADER) + "\n" + '"Buffered","X","","",""\n').encode()
    manifest = make_fixture(tmp_path, content)
    pipeline = StructuredPipeline(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)
    original_open = os.open
    opened_fds: list[int] = []

    def counted_open(file: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        fd = original_open(file, flags, *args, **kwargs)
        if file == "debarred.csv":
            opened_fds.append(fd)
        return fd

    monkeypatch.setattr(os, "open", counted_open)
    pipeline.run()

    assert pipeline.produced_result is not None
    cursor = pipeline.produced_result.populations[0]._cursors[0]
    assert len(opened_fds) == 1
    assert cursor._reader._raw.name == opened_fds[0]
    assert cursor._reader.byte_length == len(content)
    assert cursor._reader.digest.hexdigest() == hashlib.sha256(content).hexdigest()
    assert not hasattr(cursor._reader, "seek")
    assert not hasattr(cursor._reader, "rewind")
    assert cursor._reader.eof
    assert cursor.state == "verificado-completo"
    assert cursor._reader.closed


def test_pipeline_run_accepts_nonempty_records_legitimately_filtered_to_empty(
    tmp_path: Path,
) -> None:
    content = (",".join(HEADER) + "\n" + '"Decoy","X","","",""\n').encode()
    manifest = make_fixture(tmp_path, content)
    pipeline = StructuredPipeline(MagicMock(), tmp_path, manifest, discard=True)
    set_run_id_before_producer(pipeline, manifest)
    pipeline.run()
    assert pipeline.loaded_records == ()
    assert pipeline.records == []
    assert pipeline.load_calls == 1
    statuses = [
        call.args[1]["status"]
        for call in pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    ]
    assert statuses == ["running", "loaded"]


def test_pipeline_run_rejects_artifact_symlink_swapped_at_open_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    content = (",".join(HEADER) + "\n" + '"Inside","X","","",""\n').encode()
    relative_path = "nested/data/debarred.csv"
    positive_root = tmp_path / "positive"
    positive_root.mkdir()
    positive_manifest = make_fixture(positive_root, content, relative_path=relative_path)
    positive = StructuredPipeline(MagicMock(), positive_root, positive_manifest)
    set_run_id_before_producer(positive, positive_manifest)
    positive.run()
    assert positive.load_calls == 1

    race_root = tmp_path / "race"
    race_root.mkdir()
    manifest = make_fixture(race_root, content, relative_path=relative_path)
    outside = tmp_path / "outside-artifact.csv"
    outside.write_bytes(content)
    pipeline = StructuredPipeline(MagicMock(), race_root, manifest)
    set_run_id_before_producer(pipeline, manifest)

    original_path_open = Path.open
    original_os_open = os.open
    swap_performed = False
    outside_opened = False
    outside_stat = outside.stat()
    artifact_open_calls = 0

    def swap_before_actual_open() -> None:
        nonlocal swap_performed
        if not swap_performed:
            artifact = race_root / relative_path
            artifact.unlink()
            artifact.symlink_to(outside)
            swap_performed = True

    def observed_path_open(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal artifact_open_calls, outside_opened
        if path.name == "debarred.csv":
            swap_before_actual_open()
            artifact_open_calls += 1
        handle = original_path_open(path, *args, **kwargs)
        if path.name == "debarred.csv":
            opened_stat = os.fstat(handle.fileno())
            outside_opened = (opened_stat.st_dev, opened_stat.st_ino) == (
                outside_stat.st_dev,
                outside_stat.st_ino,
            )
        return handle

    def observed_os_open(file: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        nonlocal artifact_open_calls, outside_opened
        is_artifact = file == "debarred.csv" or (
            isinstance(file, (str, Path)) and Path(file).name == "debarred.csv"
        )
        if is_artifact:
            swap_before_actual_open()
            artifact_open_calls += 1
        fd = original_os_open(file, flags, *args, **kwargs)
        if is_artifact:
            opened_stat = os.fstat(fd)
            outside_opened = (opened_stat.st_dev, opened_stat.st_ino) == (
                outside_stat.st_dev,
                outside_stat.st_ino,
            )
        return fd

    monkeypatch.setattr(Path, "open", observed_path_open)
    monkeypatch.setattr(os, "open", observed_os_open)
    rejected = False
    try:
        pipeline.run()
    except ExtractionContractError:
        rejected = True

    outcome = (
        swap_performed,
        outside_opened,
        rejected,
        pipeline.records,
        pipeline.load_calls,
    )
    assert outcome == (True, False, True, [], 0)
    assert artifact_open_calls == 1
    statuses = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    assert statuses[-1].args[1]["status"] == "quality_fail"


def test_pipeline_run_rejects_intermediate_symlink_component(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    content = (",".join(HEADER) + "\n" + '"Outside","X","","",""\n').encode()
    root = tmp_path / "root"
    root.mkdir()
    relative_path = "nested/data/debarred.csv"
    manifest = make_fixture(root, content, relative_path=relative_path)
    outside_dir = tmp_path / "outside-dir"
    outside_dir.mkdir()
    outside = outside_dir / "debarred.csv"
    outside.write_bytes(content)
    nested = root / "nested"
    (nested / "data" / "debarred.csv").unlink()
    (nested / "data").rmdir()
    nested.rmdir()
    nested.symlink_to(outside_dir, target_is_directory=True)
    outside_stat = outside.stat()
    opened_outside = False
    original_os_open = os.open

    def observed_os_open(file: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        nonlocal opened_outside
        fd = original_os_open(file, flags, *args, **kwargs)
        opened_stat = os.fstat(fd)
        if (opened_stat.st_dev, opened_stat.st_ino) == (outside_stat.st_dev, outside_stat.st_ino):
            opened_outside = True
        return fd

    pipeline = StructuredPipeline(MagicMock(), root, manifest)
    set_run_id_before_producer(pipeline, manifest)
    monkeypatch.setattr(os, "open", observed_os_open)
    with pytest.raises(ExtractionContractError):
        pipeline.run()

    assert opened_outside is False
    assert pipeline.load_calls == 0
    assert pipeline.records == []
    statuses = [
        call.args[1]["status"]
        for call in pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    ]
    assert statuses == ["running", "quality_fail"]


@pytest.mark.parametrize("malformed_field", ["artifact_id", "population_id"])
def test_pipeline_run_typed_rejection_of_unhashable_manifest_identity_before_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, malformed_field: str
) -> None:
    import os

    content = (",".join(HEADER) + "\n").encode()
    manifest = make_fixture(tmp_path, content)
    data = json.loads(manifest.read_text(encoding="utf-8"))
    if malformed_field == "artifact_id":
        data["populations"][0]["artifacts"][0][malformed_field] = []
    else:
        data["populations"][0][malformed_field] = []
    manifest.write_text(json.dumps(data), encoding="utf-8")
    pipeline = StructuredPipeline(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)
    original_os_open = os.open
    artifact_open_calls = 0

    def counted_os_open(file: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        nonlocal artifact_open_calls
        if isinstance(file, (str, Path)) and Path(file).name == "debarred.csv":
            artifact_open_calls += 1
        return original_os_open(file, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", counted_os_open)
    with pytest.raises(ExtractionContractError):
        pipeline.run()

    assert artifact_open_calls == 0
    assert pipeline.load_calls == 0
    statuses = [
        call.args[1]["status"]
        for call in pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    ]
    assert statuses == ["running", "quality_fail"]


def test_pipeline_run_typed_rejection_of_nul_path_before_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    content = (",".join(HEADER) + "\n").encode()
    manifest = make_fixture(tmp_path, content)
    data = json.loads(manifest.read_text(encoding="utf-8"))
    data["populations"][0]["artifacts"][0]["relative_path"] = "nested\x00/debarred.csv"
    manifest.write_text(json.dumps(data), encoding="utf-8")
    pipeline = StructuredPipeline(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)
    original_os_open = os.open
    artifact_open_calls = 0

    def counted_os_open(file: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        nonlocal artifact_open_calls
        if file == "debarred.csv" or (
            isinstance(file, (str, Path)) and "debarred.csv" in str(file)
        ):
            artifact_open_calls += 1
        return original_os_open(file, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", counted_os_open)
    with pytest.raises(ExtractionContractError, match="unsafe artifact path"):
        pipeline.run()

    assert artifact_open_calls == 0
    assert pipeline.load_calls == 0
    statuses = [
        call.args[1]["status"]
        for call in pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    ]
    assert statuses == ["running", "quality_fail"]


def test_pipeline_run_refuses_filesystem_unencodable_path_before_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    content = (",".join(HEADER) + "\n" + '"Legal Unicode","X","","",""\n').encode()
    legal_path = "nested/café.csv"
    legal_root = tmp_path / "legal"
    legal_root.mkdir()
    legal_manifest = make_fixture(legal_root, content, relative_path=legal_path)
    legal_pipeline = StructuredPipeline(MagicMock(), legal_root, legal_manifest)
    set_run_id_before_producer(legal_pipeline, legal_manifest)
    legal_pipeline.run()
    assert legal_pipeline.load_calls == 1
    assert legal_pipeline.loaded_records[0]["Firm Name"] == "Legal Unicode"

    invalid_path = "\udc00/nested.csv"
    with pytest.raises(UnicodeEncodeError):
        os.fsencode(invalid_path)
    invalid_root = tmp_path / "invalid"
    invalid_root.mkdir()
    manifest = make_fixture(invalid_root, content)
    data = json.loads(manifest.read_text(encoding="utf-8"))
    data["populations"][0]["artifacts"][0]["relative_path"] = invalid_path
    manifest.write_text(json.dumps(data), encoding="utf-8")
    pipeline = StructuredPipeline(MagicMock(), invalid_root, manifest)
    set_run_id_before_producer(pipeline, manifest)
    original_os_open = os.open
    invalid_component_open_calls = 0

    def counted_os_open(file: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        nonlocal invalid_component_open_calls
        if file == "\udc00":
            invalid_component_open_calls += 1
        return original_os_open(file, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", counted_os_open)
    with pytest.raises(ExtractionContractError, match="filesystem encoding"):
        pipeline.run()

    assert invalid_component_open_calls == 0
    assert pipeline.load_calls == 0
    statuses = [
        call.args[1]["status"]
        for call in pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    ]
    assert statuses == ["running", "quality_fail"]


def test_invalid_identity_rejected_and_paths_cannot_escape_root(tmp_path: Path) -> None:
    from bracc_etl.extraction import validate_manifest

    bad_identity = {
        "manifest_schema_version": 1,
        "adapter_id": "world-bank-fixture",
        "adapter_version": "1",
        "run_id": "wrong",
        "source_id": "world_bank",
        "source_url": URL,
        "populations": [{"population_id": "firms", "artifacts": []}],
    }
    with pytest.raises(ExtractionContractError, match="identity"):
        validate_manifest(
            bad_identity, fixture_adapter(), run_id="right", source_id="world_bank", source_url=URL
        )
    traversal = {
        "manifest_schema_version": 1,
        "adapter_id": "world-bank-fixture",
        "adapter_version": "1",
        "run_id": "r",
        "source_id": "world_bank",
        "source_url": URL,
        "populations": [
            {
                "population_id": "firms",
                "artifacts": [
                    {
                        "artifact_id": "debarred",
                        "relative_path": "../escape.csv",
                        "format": "csv",
                        "sha256": "0" * 64,
                        "byte_length": 0,
                        "schema_id": "world_bank.debarred.fixture",
                        "schema_version": "1",
                    }
                ],
            }
        ],
    }
    with pytest.raises(ExtractionContractError, match="unsafe"):
        validate_manifest(
            traversal, fixture_adapter(), run_id="r", source_id="world_bank", source_url=URL
        )


def test_same_length_digest_mismatch_is_detected_before_load(tmp_path: Path) -> None:
    expected = (",".join(HEADER) + "\n" + '"Original","X","","",""\n').encode()
    manifest = make_fixture(tmp_path, expected)
    tampered = expected.replace(b"Original", b"Tampered")
    assert len(tampered) == len(expected)
    (tmp_path / "debarred.csv").write_bytes(tampered)
    pipeline = StructuredPipeline(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)

    with pytest.raises(ExtractionContractError, match="digest mismatch"):
        pipeline.run()

    assert pipeline.load_calls == 0
    assert pipeline.produced_result is not None
    cursor = pipeline.produced_result.populations[0]._cursors[0]
    assert cursor.state == "falha"
    assert cursor._reader.closed


def test_early_consumer_return_fails_and_closes_reader(tmp_path: Path) -> None:
    content = (
        ",".join(HEADER) + "\n" + '"First","X","","",""\n' + '"Second","X","","",""\n'
    ).encode()
    manifest = make_fixture(tmp_path, content)

    class EarlyReturnPipeline(StructuredPipeline):
        def transform_result(self, result: Any) -> tuple[dict[str, Any], ...]:
            self.result_seen = result
            return InputPlan(result)

    pipeline = EarlyReturnPipeline(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)

    pipeline.run()

    assert pipeline.load_calls == 1
    assert pipeline.produced_result is not None
    cursor = pipeline.produced_result.populations[0]._cursors[0]
    assert cursor.state == "verificado-completo"
    assert cursor._reader.closed


def test_truncated_manifested_stream_never_loads(tmp_path: Path) -> None:
    content = (",".join(HEADER) + "\n" + '"One","X","","",""\n').encode()
    manifest = make_fixture(tmp_path, content)
    (tmp_path / "debarred.csv").write_bytes(content[:-2])
    pipeline = StructuredPipeline(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)
    with pytest.raises(ExtractionContractError):
        pipeline.run()
    assert pipeline.load_calls == 0
    statuses = [
        call.args[1]["status"]
        for call in pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    ]
    assert statuses[-1] == "quality_fail"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda m: m.update(manifest_schema_version=True), "version"),
        (lambda m: m.update(adapter_id="unknown"), "identity"),
        (lambda m: m.update(source_url="https://other.invalid/data"), "identity"),
        (lambda m: m["populations"].append(m["populations"][0]), "coverage"),
        (lambda m: m["populations"].clear(), "coverage"),
    ],
)
def test_manifest_rejects_independent_version_identity_and_coverage_errors(
    tmp_path: Path, mutation: Any, message: str
) -> None:
    from bracc_etl.extraction import validate_manifest

    content = (",".join(HEADER) + "\n").encode()
    manifest_path = make_fixture(tmp_path, content)
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    data["run_id"] = "r"
    mutation(data)
    with pytest.raises(ExtractionContractError, match=message):
        validate_manifest(
            data, fixture_adapter(), run_id="r", source_id="world_bank", source_url=URL
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("byte_length", True, "digest or length"),
        ("byte_length", -1, "digest or length"),
        ("sha256", "g" * 64, "digest or length"),
        ("schema_version", "2", "schema identity"),
        ("artifact_id", "other", "coverage"),
        ("relative_path", "../escape.csv", "unsafe"),
    ],
)
def test_manifest_rejects_malformed_artifact_bindings_with_typed_refusal(
    tmp_path: Path, field: str, value: Any, message: str
) -> None:
    from bracc_etl.extraction import validate_manifest

    content = (",".join(HEADER) + "\n").encode()
    manifest_path = make_fixture(tmp_path, content)
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    data["run_id"] = "r"
    data["populations"][0]["artifacts"][0][field] = value
    with pytest.raises(ExtractionContractError, match=message):
        validate_manifest(
            data, fixture_adapter(), run_id="r", source_id="world_bank", source_url=URL
        )


@pytest.mark.parametrize(
    ("bad_population", "message"),
    [
        ([], "coverage"),
        ([{"population_id": "firms", "artifacts": []}], "coverage"),
        (
            [
                {"population_id": "firms", "artifacts": []},
                {"population_id": "firms", "artifacts": []},
            ],
            "coverage",
        ),
    ],
)
def test_manifest_rejects_missing_extra_and_duplicate_population_coverage(
    tmp_path: Path, bad_population: list[dict[str, Any]], message: str
) -> None:
    from bracc_etl.extraction import validate_manifest

    content = (",".join(HEADER) + "\n").encode()
    manifest_path = make_fixture(tmp_path, content)
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    data["run_id"] = "r"
    data["populations"] = bad_population
    with pytest.raises(ExtractionContractError, match=message):
        validate_manifest(
            data, fixture_adapter(), run_id="r", source_id="world_bank", source_url=URL
        )


def test_path_replacement_after_open_consumes_only_original_open_bytes(tmp_path: Path) -> None:
    original = (",".join(HEADER) + "\n" + '"Original","X","","",""\n').encode()
    replacement = (",".join(HEADER) + "\n" + '"Changed","X","","",""\n').encode()
    manifest = make_fixture(tmp_path, original)

    class SwapAfterProducer(StructuredPipeline):
        def transform_result(self, result: Any) -> tuple[dict[str, Any], ...]:
            replacement_path = self.root / "replacement.csv"
            replacement_path.write_bytes(replacement)
            replacement_path.replace(self.root / "debarred.csv")
            return super().transform_result(result)

    pipeline = SwapAfterProducer(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)
    pipeline.run()
    assert pipeline.loaded_records[0]["Firm Name"] == "Original"
    assert pipeline.produced_result is not None
    cursor = pipeline.produced_result.populations[0]._cursors[0]
    assert cursor._reader.digest.hexdigest() == hashlib.sha256(original).hexdigest()
    assert cursor._reader.byte_length == len(original)


def test_physical_eof_without_parser_eof_is_incomplete_and_closed(tmp_path: Path) -> None:
    content = (",".join(HEADER) + "\n").encode()
    manifest = make_fixture(tmp_path, content)

    class NoConsume(StructuredPipeline):
        def transform_result(self, result: Any) -> tuple[dict[str, Any], ...]:
            self.result_seen = result
            return InputPlan(result)

    pipeline = NoConsume(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)
    pipeline.run()
    cursor = pipeline.produced_result.populations[0]._cursors[0]
    assert cursor.state == "verificado-completo"
    assert cursor._reader.closed
    assert cursor._reader.eof


@pytest.mark.parametrize("failure", ["io", "decode", "parse", "schema", "size"])
def test_stream_and_schema_failures_quality_fail_before_load(
    tmp_path: Path, failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    if failure == "decode":
        content = b"\xff\xfe\n"
    elif failure == "parse":
        content = (",".join(HEADER) + '\n"broken\n').encode()
    elif failure == "schema":
        content = b"different,header\n"
    else:
        content = (",".join(HEADER) + "\n").encode()
    manifest = make_fixture(tmp_path, content)
    pipeline = StructuredPipeline(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)
    if failure == "io":
        import os

        original_fdopen = os.fdopen

        class BrokenReader:
            def __init__(self, wrapped: Any) -> None:
                self.wrapped = wrapped

            def read(self, size: int = -1) -> bytes:
                raise OSError("synthetic read failure")

            def close(self) -> None:
                self.wrapped.close()

        def failing_fdopen(fd: int, *args: Any, **kwargs: Any) -> Any:
            return BrokenReader(original_fdopen(fd, *args, **kwargs))

        monkeypatch.setattr(os, "fdopen", failing_fdopen)
    if failure == "size":
        data = json.loads(manifest.read_text(encoding="utf-8"))
        data["run_id"] = "placeholder"
        data["populations"][0]["artifacts"][0]["byte_length"] += 1
        manifest.write_text(json.dumps(data), encoding="utf-8")
        set_run_id_before_producer(pipeline, manifest)
    with pytest.raises((ExtractionContractError, OSError)):
        pipeline.run()
    assert pipeline.load_calls == 0
    statuses = [
        call.args[1]["status"]
        for call in pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    ]
    assert statuses == ["running", "quality_fail"]
    assert pipeline.produced_result is not None
    for cursor in pipeline.produced_result.populations[0]._cursors:
        assert cursor._reader.closed


def test_transform_cannot_replace_a_manifest_binding(tmp_path: Path) -> None:
    from dataclasses import replace

    content = (",".join(HEADER) + "\n" + '"Bound","X","","",""\n').encode()
    manifest = make_fixture(tmp_path, content)

    class MutatingBinding(StructuredPipeline):
        def transform_result(self, result: Any) -> tuple[dict[str, Any], ...]:
            self.result_seen = result
            cursor = result.populations[0]._cursors[0]
            cursor.binding = replace(cursor.binding, artifact_id="different")
            return InputPlan(result)

    pipeline = MutatingBinding(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)

    with pytest.raises(ExtractionContractError, match="binding was mutated"):
        pipeline.run()

    assert pipeline.load_calls == 0
    assert pipeline.produced_result is not None
    assert pipeline.produced_result.populations[0]._cursors[0]._reader.closed


def test_cross_run_result_is_rejected_and_its_reader_closed(tmp_path: Path) -> None:
    content = (",".join(HEADER) + "\n").encode()
    manifest = make_fixture(tmp_path, content)
    first = StructuredPipeline(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(first, manifest)
    result_and_cursors = first.produce_extraction()
    second = StructuredPipeline(MagicMock(), tmp_path, manifest)
    second.produce_extraction = lambda: result_and_cursors  # type: ignore[method-assign]

    with pytest.raises(ExtractionContractError, match="result identity"):
        second.run()

    assert second.load_calls == 0
    assert result_and_cursors[1][0]._reader.closed


def test_pipeline_rejects_a_cursor_shared_twice_and_closes_it(tmp_path: Path) -> None:
    from dataclasses import replace

    content = (",".join(HEADER) + "\n").encode()
    manifest = make_fixture(tmp_path, content)

    class SharedCursor(StructuredPipeline):
        def produce_extraction(self) -> Any:
            result, cursors = super().produce_extraction()
            population = result.populations[0]
            duplicated_population = replace(population, _cursors=(cursors[0], cursors[0]))
            duplicated_result = replace(result, populations=(duplicated_population,))
            return duplicated_result, (cursors[0], cursors[0])

    pipeline = SharedCursor(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)
    with pytest.raises(ExtractionContractError, match="sharing"):
        pipeline.run()

    assert pipeline.load_calls == 0
    assert pipeline.produced_result is not None
    assert pipeline.produced_result.populations[0]._cursors[0]._reader.closed


def test_valid_multiple_populations_are_all_verified_and_loaded(tmp_path: Path) -> None:
    from bracc_etl.extraction import open_extraction

    first = (",".join(HEADER) + "\n" + '"First","X","","",""\n').encode()
    second = (",".join(HEADER) + "\n" + '"Second","Y","","",""\n').encode()
    (tmp_path / "first.csv").write_bytes(first)
    (tmp_path / "second.csv").write_bytes(second)
    populations = (
        PopulationSpec(
            "firms", "world_bank.firms", "1", (("first", "csv"),), csv_dict_parser, HEADER
        ),
        PopulationSpec(
            "people", "world_bank.people", "1", (("second", "csv"),), csv_dict_parser, HEADER
        ),
    )
    adapter = SourceAdapterSpec("multi-fixture", "1", "world_bank", URL, 1, populations)
    artifacts = []
    for artifact_id, filename, data, schema in (
        ("first", "first.csv", first, "world_bank.firms"),
        ("second", "second.csv", second, "world_bank.people"),
    ):
        artifacts.append(
            {
                "artifact_id": artifact_id,
                "relative_path": filename,
                "format": "csv",
                "sha256": hashlib.sha256(data).hexdigest(),
                "byte_length": len(data),
                "schema_id": schema,
                "schema_version": "1",
            }
        )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "manifest_schema_version": 1,
                "adapter_id": "multi-fixture",
                "adapter_version": "1",
                "run_id": "placeholder",
                "source_id": "world_bank",
                "source_url": URL,
                "populations": [
                    {"population_id": "firms", "artifacts": [artifacts[0]]},
                    {"population_id": "people", "artifacts": [artifacts[1]]},
                ],
            }
        ),
        encoding="utf-8",
    )

    class MultiPipeline(StructuredPipeline):
        def produce_extraction(self) -> Any:
            self.extraction_adapter = adapter
            return open_extraction(
                self.root,
                self.manifest_path,
                adapter,
                run_id=self.run_id,
                source_id=self.source_identity.source_id,
                source_url=URL,
            )

        def transform_result(self, result: Any) -> tuple[dict[str, Any], ...]:
            return InputPlan(result)

    pipeline = MultiPipeline(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)
    pipeline.run()

    assert [row["Firm Name"] for row in pipeline.loaded_records] == ["First", "Second"]
    assert pipeline.load_calls == 1


def test_consumer_exception_closes_every_reader_and_never_loads(tmp_path: Path) -> None:
    content = (",".join(HEADER) + "\n" + '"Row","X","","",""\n').encode()
    manifest = make_fixture(tmp_path, content)

    class ConsumerFailure(StructuredPipeline):
        def transform_result(self, result: Any) -> tuple[dict[str, Any], ...]:
            self.result_seen = result
            raise RuntimeError("consumer failure")

    pipeline = ConsumerFailure(MagicMock(), tmp_path, manifest)
    set_run_id_before_producer(pipeline, manifest)
    with pytest.raises(RuntimeError, match="consumer failure"):
        pipeline.run()

    assert pipeline.load_calls == 0
    assert pipeline.produced_result is not None
    for cursor in pipeline.produced_result.populations[0]._cursors:
        assert cursor._reader.closed
    statuses = [
        call.args[1]["status"]
        for call in pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    ]
    assert statuses == ["running", "quality_fail"]


def test_parser_initialization_failure_closes_already_open_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from bracc_etl.extraction import open_extraction

    content = (",".join(HEADER) + "\n").encode()
    manifest = make_fixture(tmp_path, content)

    def broken_parser(reader: Any) -> Any:
        raise RuntimeError("parser initialization")

    adapter = SourceAdapterSpec(
        "world-bank-fixture",
        "1",
        "world_bank",
        URL,
        1,
        (
            PopulationSpec(
                "firms",
                "world_bank.debarred.fixture",
                "1",
                (("debarred", "csv"),),
                broken_parser,
                HEADER,
            ),
        ),
    )
    import os

    original_fdopen = os.fdopen
    handles: list[Any] = []

    def capture_fdopen(fd: int, *args: Any, **kwargs: Any) -> Any:
        handle = original_fdopen(fd, *args, **kwargs)
        handles.append(handle)
        return handle

    monkeypatch.setattr(os, "fdopen", capture_fdopen)
    with pytest.raises(RuntimeError, match="parser initialization"):
        open_extraction(
            tmp_path,
            manifest,
            adapter,
            run_id="placeholder",
            source_id="world_bank",
            source_url=URL,
        )

    assert len(handles) == 1
    assert handles[0].closed


@pytest.mark.parametrize(
    "mutation",
    [
        lambda manifest: manifest.update(adapter_version="unknown"),
        lambda manifest: manifest["populations"][0]["artifacts"].clear(),
        lambda manifest: manifest["populations"][0]["artifacts"].append(
            manifest["populations"][0]["artifacts"][0].copy()
        ),
        lambda manifest: manifest["populations"][0]["artifacts"][0].update(
            artifact_id="unexpected"
        ),
        lambda manifest: manifest["populations"][0]["artifacts"][0].update(format="unknown"),
    ],
)
def test_manifest_rejects_unknown_adapter_and_missing_extra_or_duplicate_artifacts(
    tmp_path: Path, mutation: Any
) -> None:
    from bracc_etl.extraction import validate_manifest

    content = (",".join(HEADER) + "\n").encode()
    manifest_path = make_fixture(tmp_path, content)
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    data["run_id"] = "r"
    mutation(data)

    with pytest.raises(ExtractionContractError):
        validate_manifest(
            data, fixture_adapter(), run_id="r", source_id="world_bank", source_url=URL
        )
