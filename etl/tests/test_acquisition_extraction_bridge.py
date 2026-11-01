from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from bracc_etl.acquisition.client import Acquisition, Manifest
from bracc_etl.acquisition.extraction_bridge import (
    build_extraction_manifest,
    open_acquired_extraction,
)
from bracc_etl.base import Pipeline
from bracc_etl.extraction import InputPlan, PopulationSpec, SourceAdapterSpec, csv_dict_parser

URLPATH = "/fixture.csv"
HEADER = ("id", "value")


class Handler(BaseHTTPRequestHandler):
    body = b"id,value\n1,ok\n"

    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *_: Any) -> None:
        pass


class FixturePipeline(Pipeline):
    name = "world_bank"
    source_id = "world_bank"

    def __init__(
        self,
        driver: Any,
        root: Path,
        manifest: Path,
        adapter: SourceAdapterSpec,
        acq: Manifest,
        url: str,
    ):
        super().__init__(driver, data_dir=str(root))
        self.root, self.manifest, self.extraction_adapter = root, manifest, adapter
        self.acquisition_manifest, self.configured_source_url = acq, url
        self.loaded = None
        self.alter_after_bridge = False

    def extract(self) -> None:
        raise AssertionError("structured expected")

    def transform(self) -> None:
        raise AssertionError("structured expected")

    def load(self) -> None:
        raise AssertionError("structured expected")

    def produce_extraction(self) -> Any:
        bridge = build_extraction_manifest(
            self.root,
            self.acquisition_manifest,
            self.extraction_adapter,
            run_id=self.run_id,
            source_id=self.source_identity.source_id,
            source_url=self.configured_source_url,
            page_urls={"rows.csv": self.configured_source_url},
            assignments={"rows": {"rows": ("rows.csv", "fixture.rows", "1")}},
            output_path=self.manifest,
        )
        if self.alter_after_bridge:
            artifact = self.root / "rows.csv"
            artifact.write_bytes(artifact.read_bytes().replace(b"ok", b"no"))
        return open_acquired_extraction(
            self.root,
            bridge,
            self.extraction_adapter,
            run_id=self.run_id,
            source_id=self.source_identity.source_id,
            source_url=self.configured_source_url,
        )

    def transform_result(self, result: Any) -> Any:
        return InputPlan(result)

    def load_extraction(self, records: Any) -> None:
        self.loaded = list(records)


def setup(tmp_path: Path, body: bytes = b"id,value\n1,ok\n"):
    Handler.body = body
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    port = server.server_address[1]
    root = tmp_path / "data"
    raw = root / "f05-manifest.json"
    acq = Manifest(raw, "world_bank", "fixture")
    url = f"http://127.0.0.1:{port}{URLPATH}"
    adapter = SourceAdapterSpec(
        "fixture",
        "1",
        "world_bank",
        url,
        1,
        (PopulationSpec("rows", "fixture.rows", "1", (("rows", "csv"),), csv_dict_parser, HEADER),),
    )
    try:
        Acquisition(retries=0).acquire_pages(acq, [("rows.csv", url)], root, "csv")
    except BaseException:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()
        raise
    return server, thread, root, acq, adapter, url


def stop(server: ThreadingHTTPServer, thread: threading.Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_real_acquisition_bridge_open_and_pipeline(tmp_path: Path) -> None:
    server, thread, root, acq, adapter, url = setup(tmp_path)
    try:
        f05_before = (root / "f05-manifest.json").read_bytes()
        artifact_before = (root / "rows.csv").read_bytes()
        pipeline = FixturePipeline(MagicMock(), root, root / "f06.json", adapter, acq, url)
        pipeline.run()
        bridge = pipeline.manifest
        assert bridge.exists()
        assert pipeline.loaded == [{"id": "1", "value": "ok"}]
        assert (root / "f05-manifest.json").read_bytes() == f05_before
        assert (root / "rows.csv").read_bytes() == artifact_before
        assert json.loads(bridge.read_text())["run_id"] == pipeline.run_id
    finally:
        stop(server, thread)


def test_same_size_alteration_rejected_before_loader(tmp_path: Path) -> None:
    server, thread, root, acq, adapter, url = setup(tmp_path)
    try:
        pipeline = FixturePipeline(MagicMock(), root, root / "f06.json", adapter, acq, url)
        pipeline.alter_after_bridge = True
        with pytest.raises(Exception, match="digest mismatch"):
            pipeline.run()
        assert pipeline.loaded is None
        calls = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list
        assert calls[-1].args[1]["status"] == "quality_fail"
    finally:
        stop(server, thread)


def test_assignment_traversal_and_unavailable_provenance_refused(tmp_path: Path) -> None:
    server, thread, root, acq, adapter, url = setup(tmp_path)
    try:
        with pytest.raises(Exception, match="unsafe artifact path"):
            build_extraction_manifest(
                root,
                acq,
                adapter,
                run_id="r1",
                source_id="world_bank",
                source_url=url,
                assignments={"rows": {"rows": ("../rows.csv", "fixture.rows", "1")}},
                page_urls={"../rows.csv": url},
            )
        acq.pages["rows.csv"].pop("source_sha256")
        with pytest.raises(Exception, match="URL provenance"):
            build_extraction_manifest(
                root,
                acq,
                adapter,
                run_id="r1",
                source_id="world_bank",
                source_url=url,
                assignments={"rows": {"rows": ("rows.csv", "fixture.rows", "1")}},
                page_urls={"rows.csv": url},
            )
    finally:
        stop(server, thread)


def test_explicit_url_schema_identity_and_output_bounds_are_fail_closed(tmp_path: Path) -> None:
    server, thread, root, acq, adapter, url = setup(tmp_path)
    assignment = {"rows": {"rows": ("rows.csv", "fixture.rows", "1")}}
    try:
        with pytest.raises(Exception, match="URL provenance"):
            build_extraction_manifest(
                root,
                acq,
                adapter,
                run_id="r1",
                source_id="world_bank",
                source_url=url,
                assignments=assignment,
                page_urls={"rows.csv": url + "?altered=1"},
            )
        with pytest.raises(Exception, match="schema assignment"):
            build_extraction_manifest(
                root,
                acq,
                adapter,
                run_id="r1",
                source_id="world_bank",
                source_url=url,
                assignments={"rows": {"rows": ("rows.csv", "fixture.rows", "2")}},
                page_urls={"rows.csv": url},
            )
        with pytest.raises(Exception, match="direct child"):
            build_extraction_manifest(
                root,
                acq,
                adapter,
                run_id="r1",
                source_id="world_bank",
                source_url=url,
                assignments=assignment,
                page_urls={"rows.csv": url},
                output_path=tmp_path / "outside.json",
            )
        assert not (tmp_path / "outside.json").exists()
    finally:
        stop(server, thread)


def test_checkpoint_size_is_verified_before_manifest_creation(tmp_path: Path) -> None:
    server, thread, root, acq, adapter, url = setup(tmp_path)
    try:
        checkpoint_bytes = acq.pages["rows.csv"]["bytes"]
        assert type(checkpoint_bytes) is int
        acq.pages["rows.csv"]["bytes"] = checkpoint_bytes + 1
        with pytest.raises(Exception, match="checkpoint mismatch"):
            build_extraction_manifest(
                root,
                acq,
                adapter,
                run_id="r1",
                source_id="world_bank",
                source_url=url,
                assignments={"rows": {"rows": ("rows.csv", "fixture.rows", "1")}},
                page_urls={"rows.csv": url},
                output_path=root / "f06.json",
            )
        assert not (root / "f06.json").exists()
        acq.pages["rows.csv"]["bytes"] = checkpoint_bytes
        acq.pages["rows.csv"]["sha256"] = "0" * 64
        with pytest.raises(Exception, match="checkpoint mismatch"):
            build_extraction_manifest(
                root,
                acq,
                adapter,
                run_id="r1",
                source_id="world_bank",
                source_url=url,
                assignments={"rows": {"rows": ("rows.csv", "fixture.rows", "1")}},
                page_urls={"rows.csv": url},
                output_path=root / "f06.json",
            )
    finally:
        stop(server, thread)


def test_verified_empty_csv_reaches_loader_as_observed_empty(tmp_path: Path) -> None:
    server, thread, root, acq, adapter, url = setup(tmp_path, b"id,value\n")
    try:
        pipeline = FixturePipeline(MagicMock(), root, root / "f06.json", adapter, acq, url)
        pipeline.run()
        assert pipeline.loaded == []
        call = pipeline.driver.session.return_value.__enter__.return_value.run.call_args_list[-1]
        assert call.args[1]["status"] == "loaded"
    finally:
        stop(server, thread)


def test_source_adapter_version_and_run_mismatches_are_rejected(tmp_path: Path) -> None:
    from dataclasses import replace

    server, thread, root, acq, adapter, url = setup(tmp_path)
    assignments = {"rows": {"rows": ("rows.csv", "fixture.rows", "1")}}
    try:
        with pytest.raises(Exception, match="source or URL"):
            build_extraction_manifest(
                root,
                acq,
                adapter,
                run_id="r1",
                source_id="other",
                source_url=url,
                assignments=assignments,
                page_urls={"rows.csv": url},
            )
        bridge = build_extraction_manifest(
            root,
            acq,
            adapter,
            run_id="r1",
            source_id="world_bank",
            source_url=url,
            assignments=assignments,
            page_urls={"rows.csv": url},
            output_path=root / "f06.json",
        )
        with pytest.raises(Exception, match="identity mismatch"):
            open_acquired_extraction(
                root,
                bridge,
                replace(adapter, adapter_version="2"),
                run_id="r1",
                source_id="world_bank",
                source_url=url,
            )
        with pytest.raises(Exception, match="identity mismatch"):
            open_acquired_extraction(
                root,
                bridge,
                adapter,
                run_id="other-run",
                source_id="world_bank",
                source_url=url,
            )
    finally:
        stop(server, thread)


def test_duplicate_page_assignment_is_rejected(tmp_path: Path) -> None:
    server, thread, root, acq, adapter, url = setup(tmp_path)
    duplicate_adapter = SourceAdapterSpec(
        "fixture",
        "1",
        "world_bank",
        url,
        1,
        (
            PopulationSpec(
                "rows",
                "fixture.rows",
                "1",
                (("rows", "csv"), ("rows-copy", "csv")),
                csv_dict_parser,
                HEADER,
            ),
        ),
    )
    try:
        with pytest.raises(Exception, match="duplicate page"):
            build_extraction_manifest(
                root,
                acq,
                duplicate_adapter,
                run_id="r1",
                source_id="world_bank",
                source_url=url,
                assignments={
                    "rows": {
                        "rows": ("rows.csv", "fixture.rows", "1"),
                        "rows-copy": ("rows.csv", "fixture.rows", "1"),
                    }
                },
                page_urls={"rows.csv": url},
            )
    finally:
        stop(server, thread)


def test_final_symlink_is_not_followed_or_written_outside_root(tmp_path: Path) -> None:
    server, thread, root, acq, adapter, url = setup(tmp_path)
    outside = tmp_path / "outside.csv"
    outside.write_bytes(b"id,value\n9,external\n")
    (root / "alias.csv").symlink_to(outside)
    acq.pages["alias.csv"] = dict(acq.pages["rows.csv"])
    before = outside.read_bytes()
    try:
        with pytest.raises(Exception, match="artifact missing or inaccessible"):
            build_extraction_manifest(
                root,
                acq,
                adapter,
                run_id="r1",
                source_id="world_bank",
                source_url=url,
                assignments={"rows": {"rows": ("alias.csv", "fixture.rows", "1")}},
                page_urls={"alias.csv": url},
                output_path=root / "f06.json",
            )
        assert outside.read_bytes() == before
        assert not (root / "f06.json").exists()
    finally:
        stop(server, thread)


def test_symlink_root_is_rejected_before_artifact_or_output_access(tmp_path: Path) -> None:
    server, thread, root, acq, adapter, url = setup(tmp_path)
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    f05_before = (root / "f05-manifest.json").read_bytes()
    artifact_before = (root / "rows.csv").read_bytes()
    output = alias / "f06.json"
    try:
        with pytest.raises(Exception, match="root.*symlink|symlink.*root"):
            build_extraction_manifest(
                alias,
                acq,
                adapter,
                run_id="r1",
                source_id="world_bank",
                source_url=url,
                assignments={"rows": {"rows": ("rows.csv", "fixture.rows", "1")}},
                page_urls={"rows.csv": url},
                output_path=output,
            )
        assert not (root / "f06.json").exists()
        assert not output.exists()
        assert (root / "f05-manifest.json").read_bytes() == f05_before
        assert (root / "rows.csv").read_bytes() == artifact_before
    finally:
        stop(server, thread)
