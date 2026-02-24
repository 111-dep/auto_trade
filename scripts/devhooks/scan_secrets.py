#!/usr/bin/env python3
"""Lightweight staged-file secret scanner for pre-commit.

This intentionally avoids external dependencies so it works in offline
environments.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Iterable


SKIP_SUFFIXES = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".pdf",
    ".zip",
    ".gz",
    ".tgz",
    ".pyc",
}

SKIP_PATH_PARTS = {
    ".git",
    "__pycache__",
    "logs",
    "batch_logs",
}

OKX_LINE_RE = re.compile(
    r"^\s*(OKX_API_KEY|OKX_SECRET_KEY|OKX_PASSPHRASE)\s*=\s*(.+?)\s*$"
)

GENERIC_PATTERNS = [
    ("openai_api_key", re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")),
    ("aws_access_key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("github_pat", re.compile(r"\bghp_[A-Za-z0-9]{36}\b")),
    ("private_key_header", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
]


def _normalize_value(raw: str) -> str:
    v = raw.strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1].strip()
    return v


def _is_placeholder(v: str) -> bool:
    token = v.strip().upper()
    if token in {"", "YOUR_KEY_HERE", "CHANGE_ME", "NONE", "NULL"}:
        return True
    if token.startswith("YOUR_") or token.startswith("PLACEHOLDER"):
        return True
    return False


def _should_skip(path: Path) -> bool:
    if any(part in SKIP_PATH_PARTS for part in path.parts):
        return True
    if path.suffix.lower() in SKIP_SUFFIXES:
        return True
    return False


def _iter_lines(path: Path) -> Iterable[tuple[int, str]]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    for idx, line in enumerate(text.splitlines(), start=1):
        yield idx, line


def scan_file(path: Path) -> list[str]:
    issues: list[str] = []
    for line_no, line in _iter_lines(path):
        m = OKX_LINE_RE.match(line)
        if m:
            key = m.group(1)
            value = _normalize_value(m.group(2))
            if not _is_placeholder(value):
                issues.append(f"{path}:{line_no}: potential {key} value committed")
        for name, pat in GENERIC_PATTERNS:
            if pat.search(line):
                issues.append(f"{path}:{line_no}: potential secret pattern [{name}]")
    return issues


def main(argv: list[str]) -> int:
    issues: list[str] = []
    for raw in argv[1:]:
        path = Path(raw)
        if not path.exists() or not path.is_file():
            continue
        if _should_skip(path):
            continue
        issues.extend(scan_file(path))
    if not issues:
        return 0
    print("Secret scan failed. Review the following lines:")
    for item in issues:
        print(f"  - {item}")
    print("If this is intentional test data, move it out of tracked files.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
