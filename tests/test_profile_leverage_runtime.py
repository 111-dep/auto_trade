from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from okx_trader.runtime_run_once_for_inst import _execute_decision_with_params


class ProfileLeverageRuntimeTests(unittest.TestCase):
    def test_profile_leverage_override_is_scoped_to_single_inst_execution(self) -> None:
        cfg = SimpleNamespace(params=SimpleNamespace(name="old"), leverage=10.0)
        params = SimpleNamespace(leverage=25.0, name="new")
        seen: list[float] = []

        def _fake_execute_decision(**kwargs):
            seen.append(float(kwargs["cfg"].leverage))

        with patch("okx_trader.runtime_run_once_for_inst.execute_decision", side_effect=_fake_execute_decision):
            _execute_decision_with_params(
                client=SimpleNamespace(),
                cfg=cfg,
                inst_id="XAU-USDT-SWAP",
                sig={},
                pos=SimpleNamespace(),
                state={},
                params=params,
                profile_id="XAU",
                root_state={},
            )

        self.assertEqual(seen, [25.0])
        self.assertEqual(cfg.leverage, 10.0)
        self.assertEqual(getattr(cfg.params, "name", ""), "old")

    def test_profile_leverage_zero_keeps_global(self) -> None:
        cfg = SimpleNamespace(params=SimpleNamespace(name="old"), leverage=10.0)
        params = SimpleNamespace(leverage=0.0, name="new")
        seen: list[float] = []

        def _fake_execute_decision(**kwargs):
            seen.append(float(kwargs["cfg"].leverage))

        with patch("okx_trader.runtime_run_once_for_inst.execute_decision", side_effect=_fake_execute_decision):
            _execute_decision_with_params(
                client=SimpleNamespace(),
                cfg=cfg,
                inst_id="BTC-USDT-SWAP",
                sig={},
                pos=SimpleNamespace(),
                state={},
                params=params,
                profile_id="BTCETH",
                root_state={},
            )

        self.assertEqual(seen, [10.0])
        self.assertEqual(cfg.leverage, 10.0)
        self.assertEqual(getattr(cfg.params, "name", ""), "old")


if __name__ == "__main__":
    unittest.main()
