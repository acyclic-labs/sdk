# Maintained UniFFI Python data enum typing patch

This is an isolated qualification of the pinned Mozilla UniFFI `uniffi_bindgen` 0.31.0 source. It is not a dependency pin bump and does not modify the production crate source. The external patched source is staged at `Q:\sdk\work\actors-uniffi-python-generator-patched-20261007` and its three changed generator files are retained under the external source clone.

The current Python backend keeps its runtime API: each data carrying enum remains a parent class with nested variant constructors and runtime dynamic reparenting, so `isinstance(value, SubscriptionStart)` and existing constructor names continue to work. The patch adds a generated `SubscriptionStartValue = typing.Union[SubscriptionStart.CURSOR, SubscriptionStart.CURRENT_HEAD]` alias and uses that alias in all generated type references. The dynamic reparenting runs only when `typing.TYPE_CHECKING` is false; this lets mypy and Pyright see the actual nested constructors while runtime behavior remains unchanged. Error enums are excluded from the alias path, so `BindingErrorValue` is not invented.

The generator source patch is:

- `pipeline/types.rs`: collect ordinary non-flat enum definitions, render enum references as `<Enum>Value`, and restore the enum declaration's own class name for its template.
- `pipeline/modules.rs`: export the generated value alias for ordinary data carrying enums.
- `templates/EnumTemplate.py`: emit the union alias and guard runtime reparenting with `if not typing.TYPE_CHECKING`.

Pinned generator source hashes:

- `pipeline/types.rs`: `D6435ECE1A9031BD7DA82D20FE6BB6B0AB7798744F2D5A0696C644F5D19F15F1`
- `pipeline/modules.rs`: `68EED062E3B295E2258D0395B96A1612D6895B526F85F31A9DECBE9BE3A49C72`
- `templates/EnumTemplate.py`: `6DF470CE51EC90E457D3E7D39785432476536F646F92441CF4C9E0F017899217`

The patched generator was compiled with Rust 1.98.1 and `uniffi_bindgen` 0.31.0, then run against the current Rust-owned Actors Windows native library. Generated module:

- `Q:\sdk\work\actors-uniffi-python-generator-patched-20261007\acyclic_actors_uniffi.py`
- SHA-256 `1BD8903E188767D88BB66E880FFB94D9B2896AD92E0BC9374EED9B7DFBB62FE0`

Static typing qualification used mypy 1.17.1 and Pyright 1.1.404. The valid nested `SubscriptionStart.CURSOR(1)` case passed both checkers after using the generated public `SubscriptionStartValue` annotation. The six intentional invalid cases continued to be rejected by both checkers. The Pyright positive result had zero diagnostics; mypy reported success with only its normal untyped-body note. Runtime qualification used a freshly built and installed Windows wheel containing the patched generated module and current native DLL:

- Wheel: `Q:\sdk\work\actors-uniffi-python-generator-patched-wheel-20261007\dist-patched\acyclic_actors_uniffi-0.2.0-py3-none-win_amd64.whl`
- SHA-256 `76BA00E4561C4425424562385962F9AD37EBDE724473BB28E5B1512A1E7D553B`
- Fresh constructor/runtime probe passed.
- Installed all-eight remote conformance passed against the existing live fixture, including typed service error and pre-cancelled operation checks.

The remaining product decision is whether to upstream this small generator correction or carry it as a reviewed source patch. The qualification does not claim a production dependency update or macOS qualification.
