from __future__ import annotations

from dataclasses import replace
from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs
from ..strategy_variant_legacy import resolve_variant_signal_state as legacy_resolve_variant_signal_state


def _resolve_elder_tss_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    # Keep identical behavior by delegating to the stable legacy resolver
    # with explicit variant pinning.
    kwargs = inputs.to_kwargs()
    kwargs["p"] = replace(inputs.p, strategy_variant="elder_tss_v1")
    return legacy_resolve_variant_signal_state(**kwargs)


def register(
    *,
    register_variant_resolver: Callable[[str, Callable[..., Dict[str, Any]]], None],
    register_variant_input_resolver: Callable[[str, Callable[[VariantSignalInputs], Dict[str, Any]]], None],
) -> None:
    _ = register_variant_resolver  # reserved for kwargs-based plugins
    register_variant_input_resolver("elder_tss_v1", _resolve_elder_tss_v1)
