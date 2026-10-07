# Maintained UniFFI Python data enum typing patch

This is an isolated qualification of the pinned Mozilla UniFFI `uniffi_bindgen` 0.31.0 source. It is not a dependency pin bump and does not modify the production crate source. The reviewed source patch is retained in this directory as `uniffi-python-typing.patch`; its pinned source archive metadata is recorded in `uniffi-python-typing-patch-provenance.md`, and generated outputs remain in the external `Q:\sdk\work` qualification area. Upstream source revision `309762f55db3f0548194a9ceba3027fa64b18a93` and the declared `MPL-2.0` license are recorded in `uniffi-python-typing-patch-provenance.md`.

The current Python backend keeps its runtime API: each data carrying enum remains a parent class with nested variant constructors and runtime dynamic reparenting, so `isinstance(value, SubscriptionStart)` and existing constructor names continue to work. The patch adds a generated `SubscriptionStartValue = typing.Union[SubscriptionStart.CURSOR, SubscriptionStart.CURRENT_HEAD]` alias and uses that alias in all generated type references. The dynamic reparenting runs only when `typing.TYPE_CHECKING` is false; this lets mypy and Pyright see the actual nested constructors while runtime behavior remains unchanged. Error enums are excluded from the alias path, so `BindingErrorValue` is not invented.

The generator source patch is:

- `pipeline/types.rs`: collect ordinary non-flat enum definitions, render enum references as `<Enum>Value`, and restore the enum declaration's own class name for its template.
- `pipeline/modules.rs`: export the generated value alias for ordinary data carrying enums.
- `templates/EnumTemplate.py`: emit static narrow constructor classes and the
  union alias under `typing.TYPE_CHECKING`, while guarding runtime reparenting
  so the runtime class hierarchy is unchanged.
- `templates/Async.py` and `templates/CallableBody.py`: pass the generated
  Rust future-cancel callback into the async bridge and call it when an
  in-flight Python task receives `asyncio.CancelledError`.

Pinned generator source hashes:

- `pipeline/types.rs`: `D6435ECE1A9031BD7DA82D20FE6BB6B0AB7798744F2D5A0696C644F5D19F15F1`
- `pipeline/modules.rs`: `68EED062E3B295E2258D0395B96A1612D6895B526F85F31A9DECBE9BE3A49C72`
- `templates/EnumTemplate.py`: `860D0E44F7D47BA1EE5B3F18562D174A003A8629DFAACE16DFC3BD43B99C7551`
- `templates/Async.py`: `7866EFE6AB436733ED83AAE51C1D06CADAF21077F48BF35C4E8E86F32D0F7F74`
- `templates/CallableBody.py`: `49CC9989F34624991A95A8DD3C49D515D08ECDCD46459C566A735657B910E7CF`

The patched generator was compiled with Rust 1.98.1 and `uniffi_bindgen` 0.31.0, then run against the current Rust-owned Actors Windows and Linux native libraries. The fresh generated module is byte-identical on both producers:

- `Q:\sdk\work\actors-uniffi-python-generator-patched-rerun2-20261007\acyclic_actors_uniffi.py`
- `Q:\sdk\work\actors-uniffi-python-generator-linux-patched-rerun2-20261007\acyclic_actors_uniffi.py`
- SHA-256 `20B3EEDB5FAD183ACED6DB4660B28B9738D16344B4DC652D37A3E90B47C0849A`
- Bytes `169322`

Static typing qualification used mypy 1.17.1 and Pyright 1.1.404. Both positive outputs are zero-diagnostic, and both checkers reject the six intentional invalid cases. The negative runs are expected nonzero checker invocations; their logs are marked PASS only because all six deliberate diagnostics are present. The old wheels below remain historical external evidence and are not source-bound to the fresh generator output:

- Wheel: `Q:\sdk\work\actors-uniffi-python-generator-patched-wheel-20261007\dist-patched\acyclic_actors_uniffi-0.2.0-py3-none-win_amd64.whl`
- SHA-256 `76BA00E4561C4425424562385962F9AD37EBDE724473BB28E5B1512A1E7D553B`
- The fresh static output is source-bound to the patch; a new wheel is required before claiming an installed runtime result for this exact generated module.

Static qualification covered the generated data-carrying enum alias in this Actors module (`SubscriptionStartValue`), both variant constructors, and its `SubscriptionSpec.start` field. The negative cursor-string fixture produced the expected type error in both checkers. Runtime assertions confirmed both nested constructors still satisfy `isinstance(value, SubscriptionStart)` and that the alias remains a `typing.Union` of those nested classes. The retained cross-platform receipt records fresh Linux/WSL and macOS/ivar install and relay runs, but no producer log is retained beside those patched wheels.

The maintained remote consumer is `installed-all8-remote-conformance.py`. It records each operation only after its response assertions pass, and records `remote`, `service_error`, and `pre-cancelled` only after their respective checks pass. Its optional pending-fixture path records `in-flight-task-cancellation` and `server-abort-cleanup` only after a real pending `asyncio.Task.cancel()` and observed server stream close. Receipt mode requires a producer manifest, records `artifacts.wheel`, and verifies the wheel identity before and after the run. An install check must be emitted by the install harness that actually installs the wheel.

The maintained source runner is `run-uniffi-python-typing-patch.sh`. It fetched and verified the pinned crates.io archive, applied the patch, rebuilt the generator, and reproduced the exact generated-module SHA `20B3EEDB5FAD183ACED6DB4660B28B9738D16344B4DC652D37A3E90B47C0849A`. The qualification does not claim a production dependency update.




The minimal runner was executed from clean external work directories with Rust 1.98.1 and external target/output directories on Windows and Linux/WSL. Both completed successfully and produced the same fresh generated-module SHA.

