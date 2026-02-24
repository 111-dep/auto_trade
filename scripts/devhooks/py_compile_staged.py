#!/usr/bin/env python3
"""Compile changed python files to catch syntax errors pre-commit."""

from __future__ import annotations

import py_compile
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    failed: list[str] = []
    for raw in argv[1:]:
        path = Path(raw)
        if not path.exists() or path.suffix != ".py":
            continue
        try:
            py_compile.compile(str(path), doraise=True)
        except Exception as exc:  # pragma: no cover - defensive for hook runtime
            failed.append(f"{path}: {exc}")
    if not failed:
        return 0
    print("Python compile check failed:")
    for item in failed:
        print(f"  - {item}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
