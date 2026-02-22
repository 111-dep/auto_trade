from __future__ import annotations

import os
import time
from typing import Optional


class SingleInstanceLock:
    """Process-level lock via flock to avoid multiple live runners."""

    def __init__(self, path: str):
        self.path = str(path)
        self._fh = None

    def acquire(self) -> bool:
        if not self.path:
            return False
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._fh = open(self.path, "a+", encoding="utf-8")
        try:
            import fcntl

            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception:
            try:
                self._fh.close()
            except Exception:
                pass
            self._fh = None
            return False

        try:
            self._fh.seek(0)
            self._fh.truncate(0)
            self._fh.write(f"pid={os.getpid()} started={int(time.time())}\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())
        except Exception:
            pass
        return True

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            import fcntl

            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            self._fh.close()
        except Exception:
            pass
        self._fh = None

    def __enter__(self) -> "SingleInstanceLock":
        if not self.acquire():
            raise RuntimeError(f"failed to acquire lock: {self.path}")
        return self

    def __exit__(self, exc_type, exc, tb) -> Optional[bool]:
        self.release()
        return None
