# UniFFI 0.32.2 enum typing assessment

The maintained UniFFI 0.32.2 Python template was inspected from the WSL Cargo
registry at
`uniffi_bindgen-0.32.2/src/bindings/python/templates/EnumTemplate.py`, SHA-256
`c6b5c2f6f4259ac08ebfa09d28c86e81b4ec45fa87fbe7126504e729364950b9`.

For data-carrying enums it emits a nested class for every variant and then
reassigns each variant dynamically with:

```python
SubscriptionStart.CURSOR = type("SubscriptionStart.CURSOR", (SubscriptionStart.CURSOR, SubscriptionStart), {})
```

The generated runtime relationship is correct, but mypy and Pyright analyze
the original nested class declaration and do not infer the dynamic subclass.
UniFFI 0.32.2 therefore has the same static typing behavior observed in the
0.31.0 generated facade. No local generator upgrade fixes the positive fixture.

The smallest upstream generator correction is to emit a named type alias for
non-flat enum values, for example
`SubscriptionStartValue = typing.Union[SubscriptionStart.CURSOR, SubscriptionStart.CURRENT_HEAD]`,
and use that alias in fields, constructor parameters, and converter helpers.
This keeps the runtime classes and Rust metadata unchanged while making the
actual generated variant values type-checkable. It must be implemented in the
maintained UniFFI Python template and then regenerated; this repository does
not patch generated Python output or add a parallel contract.
