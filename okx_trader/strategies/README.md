# Strategy Plugins

Drop plugin modules in this package to register new strategy variants without
editing core dispatcher code.

Module contract:

```python
def register(*, register_variant_resolver, register_variant_input_resolver) -> None:
    ...
```

- `register_variant_resolver(name, resolver)` for kwargs-based resolver.
- `register_variant_input_resolver(name, resolver)` for `VariantSignalInputs` resolver.

Only non-private modules (`*.py` without leading `_`) are auto-loaded.
