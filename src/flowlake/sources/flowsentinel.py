"""FlowSentinel as a source: its ``flows --json`` documents, or pcaps run through its CLI."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from flowlake.contract import FlowsDocument, UpstreamErrorDocument, summarize_error

# Exit codes of `flowsentinel flows` after which stdout holds a JSON document:
# 0 success, 3 rejected input, 4 malformed capture, 5 I/O error.
_JSON_EXIT_CODES = frozenset({0, 3, 4, 5})


class SourceError(Exception):
    """The input could not be read as a FlowSentinel flows document."""


class UpstreamRejected(SourceError):
    """FlowSentinel itself rejected the capture and printed an error document."""

    def __init__(self, code: str, category: str, message: str) -> None:
        super().__init__(f"{code} ({category}): {message}")
        self.code = code
        self.category = category


@dataclass(frozen=True)
class FlowsCapture:
    capture_file: str | None
    completion_state: str
    flows: list[Any]


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def parse_document(data: bytes) -> FlowsCapture:
    """Parse the JSON printed by ``flowsentinel flows --json``.

    Raises :class:`UpstreamRejected` for FlowSentinel's error document and
    :class:`SourceError` for anything that is not a flows document.
    """
    try:
        document = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceError(f"not valid JSON: {exc}") from None
    if isinstance(document, dict) and "error" in document and "flows" not in document:
        try:
            error = UpstreamErrorDocument.model_validate(document).error
        except ValidationError as exc:
            raise SourceError(f"malformed error document: {summarize_error(exc)[1]}") from None
        raise UpstreamRejected(error.code, error.category, error.message)
    try:
        parsed = FlowsDocument.model_validate(document)
    except ValidationError as exc:
        raise SourceError(f"not a FlowSentinel flows document: {summarize_error(exc)[1]}") from None
    return FlowsCapture(
        capture_file=parsed.capture.file_name,
        completion_state=parsed.completion_state,
        flows=parsed.flows,
    )


def find_binary(binary: str | None = None) -> str:
    found = binary or os.environ.get("FLOWSENTINEL_BIN") or shutil.which("flowsentinel")
    if not found:
        raise SourceError(
            "the flowsentinel binary was not found: set FLOWSENTINEL_BIN or add it to PATH"
        )
    return found


def run_flows_cli(pcap: Path, *, binary: str | None = None, timeout: float = 900.0) -> bytes:
    """Run ``flowsentinel flows --json`` on a pcap and return its stdout."""
    command = [find_binary(binary), "flows", "--json", "--pcap", str(pcap)]
    try:
        completed = subprocess.run(command, capture_output=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        raise SourceError(f"flowsentinel did not finish within {timeout:.0f} seconds") from None
    except OSError as exc:
        raise SourceError(f"could not run flowsentinel: {exc}") from None
    if completed.returncode in _JSON_EXIT_CODES and completed.stdout.strip():
        return completed.stdout
    stderr = completed.stderr.decode(errors="replace").strip()
    raise SourceError(f"flowsentinel exited with code {completed.returncode}: {stderr[:500]}")
