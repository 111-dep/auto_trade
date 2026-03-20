from __future__ import annotations

import math
import statistics
import unittest

from okx_trader.indicators import bollinger


class IndicatorTests(unittest.TestCase):
    def test_bollinger_matches_reference_pstdev(self) -> None:
        values = [
            100.0,
            101.5,
            102.0,
            99.5,
            98.0,
            100.5,
            103.0,
            102.5,
            101.0,
            104.0,
        ]
        length = 4
        mult = 2.0

        mid, up, low = bollinger(values, length, mult)

        for i in range(len(values)):
            if i < length - 1:
                self.assertIsNone(mid[i])
                self.assertIsNone(up[i])
                self.assertIsNone(low[i])
                continue

            window = values[i - length + 1 : i + 1]
            expected_mid = sum(window) / length
            expected_sd = statistics.pstdev(window)
            expected_up = expected_mid + mult * expected_sd
            expected_low = expected_mid - mult * expected_sd

            self.assertIsNotNone(mid[i])
            self.assertIsNotNone(up[i])
            self.assertIsNotNone(low[i])
            self.assertTrue(math.isclose(float(mid[i]), expected_mid, rel_tol=0.0, abs_tol=1e-12))
            self.assertTrue(math.isclose(float(up[i]), expected_up, rel_tol=0.0, abs_tol=1e-12))
            self.assertTrue(math.isclose(float(low[i]), expected_low, rel_tol=0.0, abs_tol=1e-12))


if __name__ == "__main__":
    unittest.main()
