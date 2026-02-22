from __future__ import annotations

import importlib
import pkgutil
from typing import Any, Callable, Dict

VariantResolverRegister = Callable[[str, Callable[..., Dict[str, Any]]], None]
VariantInputResolverRegister = Callable[[str, Callable[[Any], Dict[str, Any]]], None]

_PLUGINS_LOADED = False


def load_strategy_plugins(
    *,
    register_variant_resolver: VariantResolverRegister,
    register_variant_input_resolver: VariantInputResolverRegister,
) -> Dict[str, str]:
    """Auto-load strategy plugins from okx_trader.strategies.* modules.

    Plugin module contract:
    - Expose callable `register(...)`.
    - `register` accepts keyword args:
      `register_variant_resolver`, `register_variant_input_resolver`.
    """
    global _PLUGINS_LOADED
    if _PLUGINS_LOADED:
        return {}

    loaded: Dict[str, str] = {}
    for mod in pkgutil.iter_modules(__path__):  # type: ignore[name-defined]
        name = str(mod.name)
        if not name or name.startswith("_"):
            continue
        full_name = f"{__name__}.{name}"
        module = importlib.import_module(full_name)
        reg = getattr(module, "register", None)
        if not callable(reg):
            continue
        reg(
            register_variant_resolver=register_variant_resolver,
            register_variant_input_resolver=register_variant_input_resolver,
        )
        loaded[name] = full_name

    _PLUGINS_LOADED = True
    return loaded
