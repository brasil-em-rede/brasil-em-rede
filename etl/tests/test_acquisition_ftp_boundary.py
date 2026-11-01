from __future__ import annotations

import hashlib
import threading
from pathlib import Path  # noqa: TC003 - used by pytest fixture annotations

import pytest
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import DTPHandler, FTPHandler
from pyftpdlib.servers import FTPServer

from bracc_etl.acquisition import Acquisition, AcquisitionError, Manifest


@pytest.mark.parametrize("kind", ["csv", "json"])
def test_real_ftp_data_channel_interruption_resumes_without_checkpointing_later_page(
    tmp_path: Path, kind: str,
) -> None:
    """Exercise aioftp REST recovery through a deliberately cut real FTP data socket."""
    remote = tmp_path / "remote"
    remote.mkdir()
    payload = (b"name,value\n" + b"alpha,1234567890\n" * 20000 if kind == "csv"
               else b'{"entries":[' + b'{"value":"abcdefghij"},' * 12000 + b'{"end":true}]}')
    later = b'{"later":true}'
    (remote / "first").write_bytes(payload)
    (remote / "later").write_bytes(later)
    authorizer = DummyAuthorizer()
    authorizer.add_anonymous(str(remote), perm="elr")
    first_transfer = threading.Event()
    first_cut = threading.Event()
    rest_offsets: list[int] = []

    class CuttingDTP(DTPHandler):
        # Disable pyftpdlib's sendfile path so the deterministic boundary below
        # controls actual outgoing bytes, not upload-side handle_read().
        def use_sendfile(self) -> bool:
            return False

        def initiate_send(self) -> None:
            if self.file_obj.name.endswith("/first") and not first_cut.is_set():
                boundary = 131072
                sent = self.get_transmitted_bytes()
                if sent >= boundary:
                    first_transfer.set()
                    first_cut.set()
                    self.close()
                    return
                remaining = boundary - sent
                original_producer = self.producer_fifo[0]
                original_more = original_producer.more

                class CappedProducer:
                    def more(self) -> bytes:
                        chunk = original_more()
                        if len(chunk) > remaining:
                            remainder = chunk[remaining:]
                            original_producer._data = remainder
                            return chunk[:remaining]
                        return chunk

                self.producer_fifo[0] = CappedProducer()
                try:
                    super().initiate_send()
                finally:
                    self.producer_fifo[0] = original_producer
                if self.get_transmitted_bytes() >= boundary:
                    first_transfer.set()
                    first_cut.set()
                    self.close()
                return
            super().initiate_send()

    class Handler(FTPHandler):
        dtp_handler = CuttingDTP

        def ftp_REST(self, line: str) -> None:  # noqa: N802 - pyftpdlib override
            rest_offsets.append(int(line))
            super().ftp_REST(line)

    Handler.authorizer = authorizer
    ftp = FTPServer(("127.0.0.1", 0), Handler)
    server_thread = threading.Thread(
        target=ftp.serve_forever, kwargs={"timeout": 0.02, "blocking": True}, daemon=True,
    )
    server_thread.start()
    directory = tmp_path / "out"
    directory.mkdir()
    manifest_path = directory / "manifest.json"
    base = f"ftp://127.0.0.1:{ftp.socket.getsockname()[1]}"
    pages = [("first", f"{base}/first"), ("later", f"{base}/later")]
    digest = hashlib.sha256(payload).hexdigest()
    try:
        with pytest.raises(AcquisitionError, match="after retries"):
            Acquisition(retries=0, timeout=5).acquire_pages(
                Manifest.open(manifest_path, "synthetic", "boundary"), pages, directory, kind,
                {"first": digest},
            )
        assert first_transfer.is_set()
        partial = directory / "first.part"
        assert 0 < partial.stat().st_size < len(payload)
        partial_size_before_resume = partial.stat().st_size
        assert not (directory / "first").exists()
        failed_manifest = Manifest.open(manifest_path, "synthetic", "boundary")
        assert failed_manifest.pages == {}
        assert not (directory / "later").exists()
        assert rest_offsets == []

        Acquisition(retries=0, timeout=5).acquire_pages(
            Manifest.open(manifest_path, "synthetic", "boundary"), pages, directory, kind,
            {"first": digest},
        )
        assert rest_offsets == [partial_size_before_resume]
        assert (directory / "first").read_bytes() == payload
        assert hashlib.sha256((directory / "first").read_bytes()).hexdigest() == digest
        assert (directory / "later").read_bytes() == later
        complete = Manifest.open(manifest_path, "synthetic", "boundary")
        assert set(complete.pages) == {"first", "later"}
        assert complete.pages["first"]["sha256"] == digest
    finally:
        ftp.close_all()
        server_thread.join(timeout=5)
        assert not server_thread.is_alive()


@pytest.mark.parametrize("kind", ["csv", "json"])
def test_real_ftp_login_html_payload_is_not_published(tmp_path: Path, kind: str) -> None:
    remote = tmp_path / "remote"
    remote.mkdir()
    (remote / "login").write_bytes(b"<!doctype html><html><form>Sign in</form></html>")
    authorizer = DummyAuthorizer()
    authorizer.add_anonymous(str(remote), perm="elr")
    class Handler(FTPHandler):
        pass

    Handler.authorizer = authorizer
    ftp = FTPServer(("127.0.0.1", 0), Handler)
    server_thread = threading.Thread(
        target=ftp.serve_forever, kwargs={"timeout": 0.02, "blocking": True}, daemon=True,
    )
    server_thread.start()
    try:
        directory = tmp_path / "out"
        manifest = Manifest.open(directory / "manifest.json", "synthetic", "login")
        url = f"ftp://127.0.0.1:{ftp.socket.getsockname()[1]}/login"
        with pytest.raises(AcquisitionError, match="HTML"):
            Acquisition(retries=0, timeout=5).acquire_pages(
                manifest, [("page", url)], directory, kind,
            )
        assert not (directory / "page").exists()
        assert not manifest.pages
        assert Manifest.open(manifest.path, "synthetic", "login").pages == {}
    finally:
        ftp.close_all()
        server_thread.join(timeout=5)
        assert not server_thread.is_alive()
