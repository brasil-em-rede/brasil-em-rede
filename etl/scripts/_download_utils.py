"""Shared download utilities for ETL data acquisition scripts."""

from __future__ import annotations

import logging
import zipfile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import httpx

logger = logging.getLogger(__name__)


def download_file(url: str, dest: Path, *, timeout: int = 600) -> bool:
    """Download a file with streaming and safe restart support.

    Returns True on success, False on failure.
    """
    # Restart when the partial has no verified representation identity.
    partial = dest.with_suffix(dest.suffix + ".partial")
    try:
        with httpx.stream(
            "GET", url, follow_redirects=True, timeout=timeout,
            headers={"Accept-Encoding": "identity"},
        ) as response:
            response.raise_for_status()
            if response.status_code != 200:
                logger.warning("Unexpected HTTP status for %s: %s", dest.name, response.status_code)
                return False
            content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
            if content_type in {"text/html", "application/xhtml+xml"}:
                logger.warning("HTML response rejected: %s", dest.name)
                return False
            with partial.open("wb") as stream:
                for chunk in response.iter_bytes(chunk_size=65_536):
                    stream.write(chunk)
            length = response.headers.get("content-length")
            if length is not None and partial.stat().st_size != int(length):
                logger.warning("Incomplete download: %s", dest.name)
                return False
            with partial.open("rb") as stream:
                prefix = stream.read(1024).lstrip().lower()
            while prefix.startswith(b"<!--"):
                comment_end = prefix.find(b"-->", 4)
                if comment_end < 0:
                    break
                prefix = prefix[comment_end + 3:].lstrip()
            if prefix.startswith(
                (b"<!doctype html", b"<html", b"<head", b"<body", b"<form")
            ) or (prefix.startswith(b"<!--") and b"<form" in prefix):
                logger.warning("HTML response rejected: %s", dest.name)
                return False
            partial.replace(dest)
            return True
    except (httpx.HTTPError, OSError, ValueError) as e:
        logger.warning("Failed to download %s: %s", dest.name, e)
        return False


def safe_extract_zip(
    zip_path: Path,
    output_dir: Path,
    *,
    max_total_bytes: int = 50 * 1024**3,  # 50GB default (CNPJ zips are huge)
) -> list[Path]:
    """Safely extract ZIP with path traversal and bomb guards.

    Deletes corrupted ZIPs for re-download.
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            # Check for path traversal
            resolved_output = output_dir.resolve()
            for info in zf.infolist():
                target = (output_dir / info.filename).resolve()
                if not target.is_relative_to(resolved_output):
                    raise ValueError(
                        f"Path traversal detected in {zip_path.name}: {info.filename}"
                    )

            # Check total uncompressed size (zip bomb guard)
            total_size = sum(info.file_size for info in zf.infolist())
            if total_size > max_total_bytes:
                raise ValueError(
                    f"ZIP bomb guard: {zip_path.name} would extract to "
                    f"{total_size / 1e9:.1f}GB (limit: {max_total_bytes / 1e9:.1f}GB)"
                )

            names = zf.namelist()
            zf.extractall(output_dir)

        logger.info("Extracted %d files from %s", len(names), zip_path.name)
        return [output_dir / n for n in names]
    except zipfile.BadZipFile:
        logger.warning("Bad ZIP file: %s — deleting for re-download", zip_path.name)
        zip_path.unlink()
        return []


def extract_zip(zip_path: Path, output_dir: Path) -> list[Path]:
    """Extract ZIP and return list of extracted files."""
    return safe_extract_zip(zip_path, output_dir)


def validate_csv(
    path: Path,
    *,
    expected_cols: int | None = None,
    encoding: str = "latin-1",
    sep: str = ";",
) -> bool:
    """Quick validation: read first 10 rows, check encoding and column count."""
    try:
        import pandas as pd

        df = pd.read_csv(
            path,
            sep=sep,
            encoding=encoding,
            header=None,
            dtype=str,
            nrows=10,
            keep_default_na=False,
        )
        if df.empty:
            logger.warning("Empty file: %s", path.name)
            return False
        if expected_cols and len(df.columns) != expected_cols:
            logger.warning(
                "%s: expected %d cols, got %d", path.name, expected_cols, len(df.columns),
            )
            return False
        logger.info("Validated %s: %d cols, first row OK", path.name, len(df.columns))
        return True
    except Exception as e:
        logger.warning("Validation failed for %s: %s", path.name, e)
        return False
