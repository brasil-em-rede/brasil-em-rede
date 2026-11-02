import hashlib
import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from bracc_etl.pipelines.cnpj import CNPJPipeline
from bracc_etl.runner import cli


@pytest.mark.parametrize(
    ("source", "message"),
    [("does_not_exist", "Unknown source"), ("cadin", "has no implemented runner")],
)
def test_invalid_source_fails_before_driver(source: str, message: str) -> None:
    with patch("bracc_etl.runner.GraphDatabase.driver") as driver:
        result = CliRunner().invoke(cli, ["run", "--source", source, "--neo4j-password", "unused"])
    assert result.exit_code != 0
    assert message in result.output
    driver.assert_not_called()


def test_legacy_alias_dispatches_runner_and_hooks_by_runner_name() -> None:
    with (
        patch("bracc_etl.runner.GraphDatabase.driver") as driver,
        patch.object(CNPJPipeline, "run") as run,
        patch("bracc_etl.runner.run_post_load_hooks") as hooks,
    ):
        result = CliRunner().invoke(
            cli, ["run", "--source", "receita_federal", "--neo4j-password", "unused"]
        )
    assert result.exit_code == 0, result.output
    run.assert_called_once()
    assert hooks.call_args.kwargs["source"] == "cnpj"
    driver.return_value.close.assert_called_once()


def test_status_reads_old_alias_and_reports_unmapped_or_conflicting_runs() -> None:
    driver = MagicMock()
    driver.session.return_value.__enter__.return_value.run.return_value = [
        {"latest": {"source_id": "receita_federal", "status": "legacy_latest",
                    "started_at": "2026-10-27T12:00:00Z", "run_id": "old-id"}},
        {"latest": {"source_id": "cnpj", "status": "canonical_older",
                    "started_at": "2026-10-26T12:00:00Z", "run_id": "new-id"}},
        {"latest": {"source_id": "retired_unknown", "run_id": "unmapped-id"}},
        {"latest": {"source_id": "pncp_bids", "runner_name": "comprasnet",
                    "run_id": "conflicting-id"}},
    ]
    with patch("bracc_etl.runner.GraphDatabase.driver", return_value=driver):
        result = CliRunner().invoke(cli, ["sources", "--status", "--neo4j-password", "unused"])
    assert result.exit_code == 0, result.output
    assert "legacy_latest" in result.output
    assert "canonical_older" not in result.output
    assert "Unmapped ingestion run unmapped-id" in result.stderr
    assert "Conflicting historical runner_name" in result.stderr
    query = driver.session.return_value.__enter__.return_value.run.call_args.args[0]
    assert "WITH r ORDER BY r.started_at DESC, r.run_id DESC" in query
    assert "ORDER BY latest.started_at DESC, latest.run_id DESC" in query
    driver.close.assert_called_once()


def test_run_cli_rejects_legacy_empty_evidence_for_source_url(tmp_path) -> None:
    source_url = "https://example.test/data.json"
    manifest = tmp_path / "manifest.json"
    artifact = tmp_path / "page.json"
    raw = b"[]"
    artifact.write_bytes(raw)
    manifest.write_text(json.dumps({
        "source_id": "cnpj",
        "pages": {
            artifact.name: {
                "source_sha256": hashlib.sha256(source_url.encode()).hexdigest(),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
            }
        },
    }))
    driver = MagicMock()
    with (
        patch("bracc_etl.runner.GraphDatabase.driver", return_value=driver),
        patch.object(CNPJPipeline, "extract"),
        patch.object(CNPJPipeline, "transform"),
        patch.object(CNPJPipeline, "load"),
        patch("bracc_etl.runner.run_post_load_hooks"),
    ):
        result = CliRunner().invoke(
            cli,
            [
                "run", "--source", "cnpj", "--neo4j-password", "unused",
                "--source-url", source_url,
                "--empty-evidence-manifest", str(manifest),
                "--empty-evidence-artifact", str(artifact),
                "--empty-evidence-format", "json",
            ],
        )
    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)
    assert str(result.exception) == "legacy empty evidence cannot prove extracted population"
    run_calls = driver.session.return_value.__enter__.return_value.run.call_args_list
    assert [call.args[1]["status"] for call in run_calls] == ["running", "quality_fail"]


def test_run_cli_rejects_incomplete_empty_evidence_before_driver(tmp_path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    with patch("bracc_etl.runner.GraphDatabase.driver") as driver:
        result = CliRunner().invoke(
            cli,
            ["run", "--source", "cnpj", "--neo4j-password", "unused",
             "--empty-evidence-manifest", str(manifest)],
        )
    assert result.exit_code != 0
    assert "requires manifest, artifact, and format" in result.output
    driver.assert_not_called()


@pytest.mark.parametrize("failure", ["url_mismatch", "manifest_source_mismatch"])
def test_run_cli_records_quality_fail_for_invalid_empty_evidence(tmp_path, failure) -> None:
    configured_url = "https://example.test/data.json"
    manifest_url = "https://example.test/data.json"
    manifest_source = "cnpj"
    if failure == "url_mismatch":
        configured_url = "https://other.example.test/data.json"
    else:
        manifest_source = "other_source"
    manifest = tmp_path / "manifest.json"
    artifact = tmp_path / "page.json"
    raw = b"[]"
    artifact.write_bytes(raw)
    manifest.write_text(json.dumps({
        "source_id": manifest_source,
        "pages": {
            artifact.name: {
                "source_sha256": hashlib.sha256(manifest_url.encode()).hexdigest(),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
            }
        },
    }))
    driver = MagicMock()
    with (
        patch("bracc_etl.runner.GraphDatabase.driver", return_value=driver),
        patch.object(CNPJPipeline, "extract"),
        patch.object(CNPJPipeline, "transform"),
        patch.object(CNPJPipeline, "load"),
        patch("bracc_etl.runner.run_post_load_hooks"),
    ):
        result = CliRunner().invoke(
            cli,
            [
                "run", "--source", "cnpj", "--neo4j-password", "unused",
                "--source-url", configured_url,
                "--empty-evidence-manifest", str(manifest),
                "--empty-evidence-artifact", str(artifact),
                "--empty-evidence-format", "json",
            ],
        )
    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)
    assert str(result.exception) == "legacy empty evidence cannot prove extracted population"
    run_calls = driver.session.return_value.__enter__.return_value.run.call_args_list
    run_params = [call.args[1] for call in run_calls]
    assert [params["status"] for params in run_params] == ["running", "quality_fail"]
