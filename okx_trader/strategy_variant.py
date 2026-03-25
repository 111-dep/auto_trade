from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from .models import StrategyParams
from .strategy_contract import VariantSignalInputs
from .strategy_variant_legacy import (
    normalize_strategy_variant as _normalize_strategy_variant_legacy,
)
from .strategy_variant_legacy import (
    resolve_variant_signal_state_from_inputs as _resolve_variant_signal_state_legacy_from_inputs,
)
from .strategy_variant_legacy import (
    resolve_variant_signal_state as _resolve_variant_signal_state_legacy,
)

# A resolver receives the same arguments as resolve_variant_signal_state and
# returns the same signal-state dict shape.
VariantResolver = Callable[..., Dict[str, Any]]
VariantInputResolver = Callable[[VariantSignalInputs], Dict[str, Any]]

# Runtime registry: variant -> resolver.
# If a variant is not registered, it falls back to the stable legacy resolver.
_VARIANT_RESOLVERS: Dict[str, VariantResolver] = {}
_VARIANT_INPUT_RESOLVERS: Dict[str, VariantInputResolver] = {}
_VARIANT_DISPATCH_CACHE: Dict[str, tuple[Optional[VariantInputResolver], VariantResolver]] = {}
_DEFAULT_RESOLVER: VariantResolver = _resolve_variant_signal_state_legacy
_PLUGINS_LOAD_ATTEMPTED = False
_PLUGINS_LOAD_ERROR: Optional[str] = None


def normalize_strategy_variant(raw: str) -> str:
    return _normalize_strategy_variant_legacy(raw)


def register_variant_resolver(variant: str, resolver: VariantResolver) -> None:
    key = normalize_strategy_variant(variant)
    _VARIANT_RESOLVERS[key] = resolver
    _VARIANT_DISPATCH_CACHE.clear()


def register_variant_input_resolver(variant: str, resolver: VariantInputResolver) -> None:
    key = normalize_strategy_variant(variant)
    _VARIANT_INPUT_RESOLVERS[key] = resolver
    _VARIANT_DISPATCH_CACHE.clear()


def unregister_variant_resolver(variant: str) -> None:
    key = normalize_strategy_variant(variant)
    _VARIANT_RESOLVERS.pop(key, None)
    _VARIANT_DISPATCH_CACHE.clear()


def unregister_variant_input_resolver(variant: str) -> None:
    key = normalize_strategy_variant(variant)
    _VARIANT_INPUT_RESOLVERS.pop(key, None)
    _VARIANT_DISPATCH_CACHE.clear()


def clear_variant_resolvers() -> None:
    _VARIANT_RESOLVERS.clear()
    _VARIANT_INPUT_RESOLVERS.clear()
    _VARIANT_DISPATCH_CACHE.clear()


def list_variant_resolvers() -> Dict[str, str]:
    _ensure_strategy_plugins_loaded()
    out: Dict[str, str] = {}
    for k, fn in _VARIANT_RESOLVERS.items():
        out[k] = getattr(fn, "__name__", fn.__class__.__name__)
    for k, fn in _VARIANT_INPUT_RESOLVERS.items():
        out[f"{k}#inputs"] = getattr(fn, "__name__", fn.__class__.__name__)
    if _PLUGINS_LOAD_ERROR:
        out["__plugin_error__"] = _PLUGINS_LOAD_ERROR
    return out


def _get_variant_resolver(variant: str) -> VariantResolver:
    key = normalize_strategy_variant(variant)
    return _VARIANT_RESOLVERS.get(key, _DEFAULT_RESOLVER)


def _get_variant_input_resolver(variant: str) -> Optional[VariantInputResolver]:
    key = normalize_strategy_variant(variant)
    return _VARIANT_INPUT_RESOLVERS.get(key)


def _get_variant_dispatch(raw_variant: str) -> tuple[Optional[VariantInputResolver], VariantResolver]:
    key_raw = str(raw_variant or "")
    cached = _VARIANT_DISPATCH_CACHE.get(key_raw)
    if cached is not None:
        return cached
    key = normalize_strategy_variant(key_raw)
    resolved = (_VARIANT_INPUT_RESOLVERS.get(key), _VARIANT_RESOLVERS.get(key, _DEFAULT_RESOLVER))
    _VARIANT_DISPATCH_CACHE[key_raw] = resolved
    return resolved


def _ensure_strategy_plugins_loaded() -> None:
    global _PLUGINS_LOAD_ATTEMPTED, _PLUGINS_LOAD_ERROR
    if _PLUGINS_LOAD_ATTEMPTED:
        return
    _PLUGINS_LOAD_ATTEMPTED = True
    try:
        from .strategies import load_strategy_plugins

        load_strategy_plugins(
            register_variant_resolver=register_variant_resolver,
            register_variant_input_resolver=register_variant_input_resolver,
        )
    except Exception as exc:
        _PLUGINS_LOAD_ERROR = str(exc)


def resolve_variant_signal_state_from_inputs(inputs: VariantSignalInputs) -> Dict[str, Any]:
    _ensure_strategy_plugins_loaded()
    input_resolver, resolver = _get_variant_dispatch(getattr(inputs.p, "strategy_variant", "classic"))
    if input_resolver is not None:
        return input_resolver(inputs)
    if resolver is _DEFAULT_RESOLVER:
        return _resolve_variant_signal_state_legacy_from_inputs(inputs)
    return resolver(**inputs.to_kwargs())


def resolve_variant_signal_state(
    *,
    p: StrategyParams,
    bias: str,
    close: float,
    ema_value: float,
    rsi_value: float,
    macd_hist_value: float,
    atr_value: float,
    hhv: float,
    llv: float,
    exl: float,
    exh: float,
    pb_low: float,
    pb_high: float,
    h_close: float,
    h_ema_fast: float,
    h_ema_slow: float,
    width: float,
    width_avg: float,
    long_location_ok: bool,
    short_location_ok: bool,
    pullback_long: bool,
    pullback_short: bool,
    not_chasing_long: bool,
    not_chasing_short: bool,
    prev_hhv: Optional[float] = None,
    prev_llv: Optional[float] = None,
    prev_exl: Optional[float] = None,
    prev_exh: Optional[float] = None,
    current_high: Optional[float] = None,
    current_low: Optional[float] = None,
    prev_high: Optional[float] = None,
    prev_low: Optional[float] = None,
    prev2_high: Optional[float] = None,
    prev2_low: Optional[float] = None,
    prev3_high: Optional[float] = None,
    prev3_low: Optional[float] = None,
    current_open: Optional[float] = None,
    prev_open: Optional[float] = None,
    prev_close: Optional[float] = None,
    upper_band: Optional[float] = None,
    lower_band: Optional[float] = None,
    mid_band: Optional[float] = None,
    prev_macd_hist: Optional[float] = None,
    volume: Optional[float] = None,
    volume_avg: Optional[float] = None,
    prev_day_high: Optional[float] = None,
    prev_day_low: Optional[float] = None,
    prev_day_close: Optional[float] = None,
    day_high_so_far: Optional[float] = None,
    day_low_so_far: Optional[float] = None,
    prev_h_ema_fast: Optional[float] = None,
    prev_h_ema_slow: Optional[float] = None,
    recent_rsi_min: Optional[float] = None,
    recent_rsi_max: Optional[float] = None,
    prev_ema_value: Optional[float] = None,
    ema_slow_value: Optional[float] = None,
    prev_ema_slow_value: Optional[float] = None,
    loc_close: Optional[float] = None,
    loc_ema_fast: Optional[float] = None,
    loc_ema_slow: Optional[float] = None,
    prev_loc_ema_fast: Optional[float] = None,
    prev_loc_ema_slow: Optional[float] = None,
    loc_rsi_value: Optional[float] = None,
    loc_atr_value: Optional[float] = None,
    loc_current_high: Optional[float] = None,
    loc_current_low: Optional[float] = None,
    hour_open: Optional[float] = None,
    hour_high: Optional[float] = None,
    hour_low: Optional[float] = None,
    hour_close: Optional[float] = None,
    hour_prev_close: Optional[float] = None,
    hour_rsi_value: Optional[float] = None,
) -> Dict[str, Any]:
    # Keep this compatibility function so old callers/resolvers keep working.
    inputs = VariantSignalInputs(
        p=p,
        bias=bias,
        close=close,
        ema_value=ema_value,
        rsi_value=rsi_value,
        macd_hist_value=macd_hist_value,
        atr_value=atr_value,
        hhv=hhv,
        llv=llv,
        exl=exl,
        exh=exh,
        pb_low=pb_low,
        pb_high=pb_high,
        h_close=h_close,
        h_ema_fast=h_ema_fast,
        h_ema_slow=h_ema_slow,
        width=width,
        width_avg=width_avg,
        long_location_ok=long_location_ok,
        short_location_ok=short_location_ok,
        pullback_long=pullback_long,
        pullback_short=pullback_short,
        not_chasing_long=not_chasing_long,
        not_chasing_short=not_chasing_short,
        prev_hhv=prev_hhv,
        prev_llv=prev_llv,
        prev_exl=prev_exl,
        prev_exh=prev_exh,
        current_high=current_high,
        current_low=current_low,
        prev_high=prev_high,
        prev_low=prev_low,
        prev2_high=prev2_high,
        prev2_low=prev2_low,
        prev3_high=prev3_high,
        prev3_low=prev3_low,
        current_open=current_open,
        prev_open=prev_open,
        prev_close=prev_close,
        upper_band=upper_band,
        lower_band=lower_band,
        mid_band=mid_band,
        prev_macd_hist=prev_macd_hist,
        volume=volume,
        volume_avg=volume_avg,
        prev_day_high=prev_day_high,
        prev_day_low=prev_day_low,
        prev_day_close=prev_day_close,
        day_high_so_far=day_high_so_far,
        day_low_so_far=day_low_so_far,
        prev_h_ema_fast=prev_h_ema_fast,
        prev_h_ema_slow=prev_h_ema_slow,
        recent_rsi_min=recent_rsi_min,
        recent_rsi_max=recent_rsi_max,
        prev_ema_value=prev_ema_value,
        ema_slow_value=ema_slow_value,
        prev_ema_slow_value=prev_ema_slow_value,
        loc_close=loc_close,
        loc_ema_fast=loc_ema_fast,
        loc_ema_slow=loc_ema_slow,
        prev_loc_ema_fast=prev_loc_ema_fast,
        prev_loc_ema_slow=prev_loc_ema_slow,
        loc_rsi_value=loc_rsi_value,
        loc_atr_value=loc_atr_value,
        loc_current_high=loc_current_high,
        loc_current_low=loc_current_low,
        hour_open=hour_open,
        hour_high=hour_high,
        hour_low=hour_low,
        hour_close=hour_close,
        hour_prev_close=hour_prev_close,
        hour_rsi_value=hour_rsi_value,
    )
    return resolve_variant_signal_state_from_inputs(inputs)
