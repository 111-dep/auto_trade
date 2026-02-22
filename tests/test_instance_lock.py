from __future__ import annotations

import tempfile
import unittest

from okx_trader.instance_lock import SingleInstanceLock


class InstanceLockTests(unittest.TestCase):
    def test_single_instance_lock(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock_path = f"{td}/trader.lock"
            lock_a = SingleInstanceLock(lock_path)
            lock_b = SingleInstanceLock(lock_path)

            self.assertTrue(lock_a.acquire())
            self.assertFalse(lock_b.acquire())

            lock_a.release()
            self.assertTrue(lock_b.acquire())
            lock_b.release()


if __name__ == "__main__":
    unittest.main()
