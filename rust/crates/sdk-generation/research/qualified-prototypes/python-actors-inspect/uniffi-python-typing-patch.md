# Maintained UniFFI Python data enum typing patch

This is an isolated qualification of the pinned Mozilla UniFFI `uniffi_bindgen` 0.31.0 source. It is not a dependency pin bump and does not modify the production crate source. The reviewed source patch is retained in this directory as `uniffi-python-typing.patch`; its pinned source archive metadata is recorded in `uniffi-python-typing-patch-provenance.md`, and generated outputs remain in the external `Q:\sdk\work` qualification area. Upstream source revision `309762f55db3f0548194a9ceba3027fa64b18a93` and the declared `MPL-2.0` license are recorded in `uniffi-python-typing-patch-provenance.md`.

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

Static qualification covered every generated data-carrying enum alias in this Actors module (`SubscriptionStartValue`), both variant constructors, and its `SubscriptionSpec.start` field. The positive all-fields fixture passed mypy 1.17.1 and Pyright 1.1.404 with zero diagnostics. The negative cursor-string fixture produced the expected type error in both checkers. Runtime assertions confirmed both nested constructors still satisfy `isinstance(value, SubscriptionStart)` and that the alias remains a `typing.Union` of those nested classes.

The source-only pending cancellation probe is `python-asyncio-pending-cancellation.py`. Against the live pending fixture at `https://localhost:60389`, it ran three real `asyncio.Task.cancel()` cycles. Each cycle observed the service transition from `active=1` to `aborted` with `active=0`, saw one continuation-map entry while pending, and returned to map size zero. The terminal marker was `PYTHON_ASYNCIO_PENDING_CANCELLATION_PASS`; the final map size was zero. This qualifies Python consumer cancellation propagation through the Rust-owned `CancellationHandle` and cleanup, not a synthetic local future.

The maintained source runner is `run-uniffi-python-typing-patch.sh`. It fetched and verified the pinned crates.io archive, applied the patch, rebuilt the generator, and reproduced the exact generated-module SHA `1BD8903E188767D88BB66E880FFB94D9B2896AD92E0BC9374EED9B7DFBB62FE0`. The qualification does not claim a production dependency update.




The minimal runner was executed from a clean external work directory with Rust 1.98.1 and an external target/output directory. It completed successfully and its generated module matched the previously qualified SHA exactly.

