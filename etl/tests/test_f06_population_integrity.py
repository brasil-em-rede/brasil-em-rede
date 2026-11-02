from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

if TYPE_CHECKING:
    from collections.abc import Sequence

from bracc_etl.base import Pipeline
from bracc_etl.extraction import (
    ExtractionContractError,
    PopulationSpec,
    SourceAdapterSpec,
    csv_dict_parser,
)

HEADER = ("id", "value")
URL = "https://fixture.invalid/populations.csv"


def _adapter() -> SourceAdapterSpec:
    return SourceAdapterSpec(
        "population-fixture",
        "1",
        "world_bank",
        URL,
        1,
        (
            PopulationSpec(
                "required", "fixture.required", "1", (("required", "csv"),), csv_dict_parser, HEADER
            ),
            PopulationSpec(
                "optional",
                "fixture.optional",
                "1",
                (("optional", "csv"),),
                csv_dict_parser,
                HEADER,
                required=False,
            ),
        ),
    )


def _write_fixture(root: Path, *, required_rows: list[str], optional_rows: list[str]) -> Path:
    populations = []
    for population_id, artifact_id, rows in (
        ("required", "required", required_rows),
        ("optional", "optional", optional_rows),
    ):
        content = (",".join(HEADER) + "\n" + "".join(f"{row}\n" for row in rows)).encode()
        filename = f"{artifact_id}.csv"
        (root / filename).write_bytes(content)

        populations.append(
            {
                "population_id": population_id,
                "artifacts": [
                    {
                        "artifact_id": artifact_id,
                        "relative_path": filename,
                        "format": "csv",
                        "sha256": hashlib.sha256(content).hexdigest(),
                        "byte_length": len(content),
                        "schema_id": f"fixture.{population_id}",
                        "schema_version": "1",
                    }
                ],
            }
        )
    manifest = root / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "manifest_schema_version": 1,
                "adapter_id": "population-fixture",
                "adapter_version": "1",
                "run_id": "placeholder",
                "source_id": "world_bank",
                "source_url": URL,
                "populations": populations,
            }
        ),
        encoding="utf-8",
    )
    return manifest


class PipelineFixture(Pipeline):
    name = "world_bank"
    source_id = "world_bank"

    def __init__(self, driver: Any, root: Path, manifest: Path, *, mode: str):
        super().__init__(driver, data_dir=str(root))
        self.root, self.manifest_path, self.mode = root, manifest, mode
        self.configured_source_url = URL
        self.extraction_adapter = _adapter()
        self.loaded: list[dict[str, Any]] | None = None

    def extract(self) -> None:
        raise AssertionError("structured path expected")

    def transform(self) -> None:
        raise AssertionError("structured path expected")

    def load(self) -> None:
        raise AssertionError("structured path expected")

    def produce_extraction(self) -> Any:
        from bracc_etl.extraction import open_extraction

        data = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        data["run_id"] = self.run_id
        self.manifest_path.write_text(json.dumps(data), encoding="utf-8")
        return open_extraction(
            self.root,
            self.manifest_path,
            _adapter(),
            run_id=self.run_id,
            source_id=self.source_identity.source_id,
            source_url=URL,
        )

    def transform_result(self, result: Any) -> Any:
        from bracc_etl.extraction import FilterPlan, InputPlan

        return FilterPlan(InputPlan(result), "id", "eq", "1")

    def load_extraction(self, records: Sequence[dict[str, Any]]) -> None:
        self.loaded = list(records)


@pytest.mark.parametrize("rows", [["1,a", "2,b"], ["1,a"]])
def test_raw_sequence_output_is_rejected_before_loader(tmp_path: Path, rows: list[str]) -> None:
    _write_fixture(tmp_path, required_rows=rows, optional_rows=[])

    class RawPipeline(PipelineFixture):
        def transform_result(self, result: Any) -> Any:
            return ({"id": "unrelated", "value": "fabricated"},)

    pipeline = RawPipeline(MagicMock(), tmp_path, tmp_path / "manifest.json", mode="raw")
    with pytest.raises(ExtractionContractError, match="must return a TransformPlan"):
        pipeline.run()
    assert pipeline.loaded is None
    calls = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    assert calls[-1].args[1]["status"] == "quality_fail"


def test_empty_required_population_is_not_hidden_by_nonempty_optional_population(
    tmp_path: Path,
) -> None:
    _write_fixture(tmp_path, required_rows=[], optional_rows=["9,optional"])
    pipeline = PipelineFixture(
        MagicMock(), tmp_path, tmp_path / "manifest.json", mode="optional-only"
    )
    with pytest.raises(
        ExtractionContractError, match="empty required population is hidden by optional records"
    ):
        pipeline.run()
    assert pipeline.loaded is None
    calls = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    assert calls[-1].args[1]["status"] == "quality_fail"


def test_adapter_policy_cannot_be_substituted_after_extraction(tmp_path: Path) -> None:
    _write_fixture(tmp_path, required_rows=["1,a"], optional_rows=[])

    class SubstitutePolicy(PipelineFixture):
        def transform_result(self, result: Any) -> Any:
            from bracc_etl.extraction import FilterPlan, InputPlan

            self.extraction_adapter = SourceAdapterSpec(
                "population-fixture",
                "1",
                "world_bank",
                URL,
                1,
                tuple(
                    PopulationSpec(
                        p.population_id,
                        p.schema_id,
                        p.schema_version,
                        p.required_artifacts,
                        p.parser,
                        p.schema,
                        required=False,
                    )
                    for p in _adapter().populations
                ),
            )
            return FilterPlan(InputPlan(result), "id", "eq", "1")

    pipeline = SubstitutePolicy(MagicMock(), tmp_path, tmp_path / "manifest.json", mode="ok")
    with pytest.raises(ExtractionContractError, match="adapter policy binding"):
        pipeline.run()
    assert pipeline.loaded is None
    calls = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    assert [call.args[1]["status"] for call in calls] == ["running", "quality_fail"]


def test_multiple_required_populations_allow_verified_zero_together(tmp_path: Path) -> None:
    adapter = SourceAdapterSpec(
        "population-fixture",
        "1",
        "world_bank",
        URL,
        1,
        tuple(
            PopulationSpec(
                f"required-{i}",
                f"fixture.required.{i}",
                "1",
                ((f"required-{i}", "csv"),),
                csv_dict_parser,
                HEADER,
            )
            for i in (1, 2)
        ),
    )
    populations = []
    for i in (1, 2):
        filename = f"required-{i}.csv"
        content = (",".join(HEADER) + "\n").encode()
        (tmp_path / filename).write_bytes(content)
        populations.append(
            {
                "population_id": f"required-{i}",
                "artifacts": [
                    {
                        "artifact_id": f"required-{i}",
                        "relative_path": filename,
                        "format": "csv",
                        "sha256": hashlib.sha256(content).hexdigest(),
                        "byte_length": len(content),
                        "schema_id": f"fixture.required.{i}",
                        "schema_version": "1",
                    }
                ],
            }
        )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "manifest_schema_version": 1,
                "adapter_id": "population-fixture",
                "adapter_version": "1",
                "run_id": "placeholder",
                "source_id": "world_bank",
                "source_url": URL,
                "populations": populations,
            }
        ),
        encoding="utf-8",
    )

    class ZeroPipeline(PipelineFixture):
        def produce_extraction(self) -> Any:
            data = json.loads(manifest.read_text(encoding="utf-8"))
            data["run_id"] = self.run_id
            manifest.write_text(json.dumps(data), encoding="utf-8")
            from bracc_etl.extraction import open_extraction

            return open_extraction(
                tmp_path,
                manifest,
                adapter,
                run_id=self.run_id,
                source_id=self.source_identity.source_id,
                source_url=URL,
            )

        def transform_result(self, result: Any) -> Any:
            from bracc_etl.extraction import InputPlan

            return InputPlan(result)

    pipeline = ZeroPipeline(MagicMock(), tmp_path, manifest, mode="zero")
    pipeline.extraction_adapter = adapter
    pipeline.run()
    calls = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    assert [call.args[1]["status"] for call in calls] == ["running", "loaded"]


def test_legacy_empty_evidence_remains_fail_closed_without_writes() -> None:
    class Legacy(Pipeline):
        name = "world_bank"
        source_id = "world_bank"

        def extract(self) -> None:
            pass

        def transform(self) -> None:
            pass

        def load(self) -> None:
            self.empty_load_evidence = object()  # type: ignore[assignment]

    pipeline = Legacy(MagicMock())
    with pytest.raises(ValueError, match="empty load requires structured extraction evidence"):
        pipeline.run()
    calls = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    assert [call.args[1]["status"] for call in calls] == ["running", "quality_fail"]
    assert pipeline.load_metrics.submitted == 0
    assert pipeline.load_metrics.direct_updates == 0


def test_runner_root_precondition_is_invocation_context_not_pipeline_product_behavior() -> None:
    """ROOT-001 is addressed by a correctly rooted invocation, not code changes."""
    import subprocess

    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], cwd=root, capture_output=True, text=True
    )
    assert result.returncode == 0
    assert Path(result.stdout.strip()) == root


def test_forged_extraction_result_cannot_reach_loader(tmp_path: Path) -> None:
    from dataclasses import replace

    _write_fixture(tmp_path, required_rows=["1,real"], optional_rows=[])

    class ForgedPipeline(PipelineFixture):
        def produce_extraction(self) -> Any:
            result, cursors = super().produce_extraction()
            self.forged_result = replace(result, _library_authority=None)
            return self.forged_result, cursors

        def transform_result(self, result: Any) -> Any:
            from bracc_etl.extraction import FilterPlan, InputPlan

            return FilterPlan(InputPlan(result), "id", "eq", "1")

    pipeline = ForgedPipeline(MagicMock(), tmp_path, tmp_path / "manifest.json", mode="forged")
    with pytest.raises(ExtractionContractError, match="not issued by the library"):
        pipeline.run()

    assert pipeline.loaded is None
    calls = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    assert [call.args[1]["status"] for call in calls] == ["running", "quality_fail"]


@pytest.mark.parametrize("optional_rows", [[], ["3,99\\n"]])
def test_pipeline_run_filters_and_aggregates_two_required_populations_with_optional_input(
    tmp_path: Path, optional_rows: list[str]
) -> None:
    from bracc_etl.extraction import (
        FieldExpr,
        FilterPlan,
        GroupAggregatePlan,
        InputPlan,
        SelectPlan,
        open_extraction,
    )

    columns = ("id", "value")
    populations = (
        PopulationSpec(
            "required-a", "fixture.required-a", "1", (("a", "csv"),), csv_dict_parser, columns
        ),
        PopulationSpec(
            "required-b", "fixture.required-b", "1", (("b", "csv"),), csv_dict_parser, columns
        ),
        PopulationSpec(
            "optional",
            "fixture.optional",
            "1",
            (("optional", "csv"),),
            csv_dict_parser,
            columns,
            required=False,
        ),
    )
    adapter = SourceAdapterSpec("population-fixture", "1", "world_bank", URL, 1, populations)
    manifest_populations = []
    for pid, artifact, rows in (
        ("required-a", "a", ["1,2\n"]),
        ("required-b", "b", ["2,3\n"]),
        ("optional", "optional", optional_rows),
    ):
        content = ("id,value\n" + "".join(rows)).encode()
        filename = f"{artifact}.csv"
        (tmp_path / filename).write_bytes(content)
        manifest_populations.append(
            {
                "population_id": pid,
                "artifacts": [
                    {
                        "artifact_id": artifact,
                        "relative_path": filename,
                        "format": "csv",
                        "sha256": hashlib.sha256(content).hexdigest(),
                        "byte_length": len(content),
                        "schema_id": f"fixture.{pid}",
                        "schema_version": "1",
                    }
                ],
            }
        )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "manifest_schema_version": 1,
                "adapter_id": "population-fixture",
                "adapter_version": "1",
                "run_id": "placeholder",
                "source_id": "world_bank",
                "source_url": URL,
                "populations": manifest_populations,
            }
        ),
        encoding="utf-8",
    )

    class LineagePipeline(PipelineFixture):
        def __init__(self, driver: Any, root: Path, manifest_path: Path):
            super().__init__(driver, root, manifest_path, mode="lineage")
            self.extraction_adapter = adapter
            self.extraction_result = None

        def produce_extraction(self) -> Any:
            data = json.loads(self.manifest_path.read_text(encoding="utf-8"))
            data["run_id"] = self.run_id
            self.manifest_path.write_text(json.dumps(data), encoding="utf-8")
            result = open_extraction(
                self.root,
                self.manifest_path,
                adapter,
                run_id=self.run_id,
                source_id="world_bank",
                source_url=URL,
            )
            self.extraction_result = result[0]
            return result

        def transform_result(self, result: Any) -> Any:
            filtered = FilterPlan(InputPlan(result), "id", "in", ("1", "2"))
            selected = SelectPlan(filtered, (FieldExpr("id", "bucket"), "value"))
            return GroupAggregatePlan(selected, ("bucket",), (("rows", "count", None),))

    pipeline = LineagePipeline(MagicMock(), tmp_path, manifest)
    pipeline.run()

    assert pipeline.loaded == [{"bucket": "1", "rows": 1}, {"bucket": "2", "rows": 1}]
    assert pipeline.extraction_result is not None
    assert [
        cursor.state
        for population in pipeline.extraction_result.populations
        for cursor in population._cursors
    ] == ["verificado-completo"] * 3
    calls = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    assert [call.args[1]["status"] for call in calls] == ["running", "loaded"]


def _write_filter_boundary_fixture(
    root: Path, records: list[dict[str, Any]]
) -> tuple[Path, SourceAdapterSpec]:

    contents = {"required": b"verified required artifact", "optional": b""}
    populations = []
    for population_id, content in contents.items():
        filename = f"{population_id}.bin"
        (root / filename).write_bytes(content)
        populations.append(
            {
                "population_id": population_id,
                "artifacts": [
                    {
                        "artifact_id": population_id,
                        "relative_path": filename,
                        "format": "custom",
                        "sha256": hashlib.sha256(content).hexdigest(),
                        "byte_length": len(content),
                        "schema_id": f"fixture.{population_id}",
                        "schema_version": "1",
                    }
                ],
            }
        )
    manifest = root / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "manifest_schema_version": 1,
                "adapter_id": "population-fixture",
                "adapter_version": "1",
                "run_id": "placeholder",
                "source_id": "world_bank",
                "source_url": URL,
                "populations": populations,
            }
        ),
        encoding="utf-8",
    )

    def required_parser(reader: Any) -> Any:
        reader.cursor.mark_schema_seen()
        reader.read()
        yield from records

    def optional_parser(reader: Any) -> Any:
        reader.cursor.mark_schema_seen()
        reader.read()
        yield from ()

    adapter = SourceAdapterSpec(
        "population-fixture",
        "1",
        "world_bank",
        URL,
        1,
        (
            PopulationSpec(
                "required", "fixture.required", "1", (("required", "custom"),), required_parser
            ),
            PopulationSpec(
                "optional",
                "fixture.optional",
                "1",
                (("optional", "custom"),),
                optional_parser,
                required=False,
            ),
        ),
    )
    return manifest, adapter


def _assert_filter_fixture_consumed(pipeline: Any, required_records: int) -> None:
    result = pipeline.extraction_result
    cursors = [cursor for population in result.populations for cursor in population._cursors]
    assert [cursor.records_seen for cursor in cursors] == [required_records, 0]
    for cursor in cursors:
        assert cursor.state == "verificado-completo"
        assert cursor._reader.eof
        assert cursor._reader.byte_length == cursor.binding.byte_length
        assert cursor._reader.digest.hexdigest() == cursor.binding.sha256


@pytest.mark.parametrize(
    ("operation", "value"), [("is_null", None), ("eq", None), ("not_null", None)]
)
def test_pipeline_rejects_absent_filter_field_after_late_row_before_loader(
    tmp_path: Path, operation: str, value: Any
) -> None:
    from bracc_etl.extraction import FilterPlan, InputPlan, open_extraction

    records = [
        {"id": "valid", "value": "present"},
        {"id": "late-invalid", "trigger": True},
    ]
    manifest, adapter = _write_filter_boundary_fixture(tmp_path, records)

    class MissingFieldPipeline(PipelineFixture):
        def produce_extraction(self) -> Any:
            data = json.loads(manifest.read_text(encoding="utf-8"))
            data["run_id"] = self.run_id
            manifest.write_text(json.dumps(data), encoding="utf-8")
            result = open_extraction(
                tmp_path, manifest, adapter, run_id=self.run_id,
                source_id="world_bank", source_url=URL
            )
            self.extraction_result = result[0]
            return result

        def transform_result(self, result: Any) -> Any:
            return FilterPlan(InputPlan(result), "value", operation, value)

    pipeline = MissingFieldPipeline(MagicMock(), tmp_path, manifest, mode="missing-field")
    pipeline.extraction_adapter = adapter
    with pytest.raises(ExtractionContractError, match="filter field is missing"):
        pipeline.run()
    assert pipeline.loaded is None
    _assert_filter_fixture_consumed(pipeline, required_records=2)
    calls = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    assert [call.args[1]["status"] for call in calls] == ["running", "quality_fail"]
    assert "filter field is missing" in calls[-1].args[1]["error"]


@pytest.mark.parametrize(
    ("operation", "expected"),
    [("is_null", ["null"]), ("not_null", ["false", "zero", "empty"])],
)
def test_pipeline_filter_distinguishes_explicit_null_from_present_falsey_values(
    tmp_path: Path, operation: str, expected: list[str]
) -> None:
    from bracc_etl.extraction import FilterPlan, InputPlan, open_extraction

    records = [
        {"id": "null", "value": None},
        {"id": "false", "value": False},
        {"id": "zero", "value": 0},
        {"id": "empty", "value": ""},
    ]
    manifest, adapter = _write_filter_boundary_fixture(tmp_path, records)

    class NullPipeline(PipelineFixture):
        def produce_extraction(self) -> Any:
            data = json.loads(manifest.read_text(encoding="utf-8"))
            data["run_id"] = self.run_id
            manifest.write_text(json.dumps(data), encoding="utf-8")
            result = open_extraction(
                tmp_path, manifest, adapter, run_id=self.run_id,
                source_id="world_bank", source_url=URL
            )
            self.extraction_result = result[0]
            return result

        def transform_result(self, result: Any) -> Any:
            return FilterPlan(InputPlan(result), "value", operation, None)

    pipeline = NullPipeline(MagicMock(), tmp_path, manifest, mode=operation)
    pipeline.extraction_adapter = adapter
    pipeline.run()
    assert [row["id"] for row in pipeline.loaded or []] == expected
    _assert_filter_fixture_consumed(pipeline, required_records=4)
    calls = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
    assert [call.args[1]["status"] for call in calls] == ["running", "loaded"]
