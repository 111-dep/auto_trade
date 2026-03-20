from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs
from ..strategy_variant_legacy import resolve_variant_signal_state_from_inputs as legacy_resolve_variant_signal_state_from_inputs


def _resolve_elder_tss_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    return legacy_resolve_variant_signal_state_from_inputs(inputs)


def register(
    *,
    register_variant_resolver: Callable[[str, Callable[..., Dict[str, Any]]], None],
    register_variant_input_resolver: Callable[[str, Callable[[VariantSignalInputs], Dict[str, Any]]], None],
) -> None:
    _ = register_variant_resolver  # reserved for kwargs-based plugins
    register_variant_input_resolver("elder_tss_v1", _resolve_elder_tss_v1)
