# Session model routing

These on-demand models use the production synchronization primitives. From the
repository root, the explicit compiler routing is:

```sh
RUSTC_WORKSPACE_WRAPPER=./scripts/loom-rustc.sh cargo test --locked -p acyclic-stream-models --features model-checking --test session_model
```

On bb2, pass the wrapper explicitly through `bs.ps1` as
`--env RUSTC_WORKSPACE_WRAPPER=/work/src/scripts/loom-rustc.sh`. An ordinary Rust
compile without that routing deliberately refuses this model target. Compiling
the target does not execute or qualify its schedules or negative controls.

The workspace consolidation exposed stale consumers of removed production APIs.
The model mapping follows the current main contract, also inspected by the
Stream owner at `b5899587789ad2152b29beaab1ee85e843a25645`:

| Retained history | Current production seam and oracle |
| --- | --- |
| Gate registration, omitted notification, wake under lock | Included `session/gate.rs`; `poll_changed(false)` observes the completed true setter. |
| Independent delivery with a saturated source; global-gate negative | `session/sources/model/admission.rs` constructs the actual `Channel`, calls `source_mode`, and traverses `Sources::next_with_control` and its queues using `policy::room`. It checks the healthy record and preserves the blocked provider's queued record. |
| Disable a parked source; keep-polling negative | The same production source queues stop consumption after credit removal. Re-admission must deliver the original queued record. |
| Generated reserved-slot capacities | The existing frame/record-size inputs exercise actual source admission for uncredited latest, credited ordinary, and globally full cases. |
| Cursor movement and stale generation | Included `client/progress.rs`, with the real generated `acyclic_stream::wire::SessionCursor`. |
| Atomic anchored resume cut; split-observation negative | Both `consume_record` and `consume_progress` race `fence` and `subscription`. Sequence and preceding immutable commit must move together; both stale consumption methods must refuse. |
| Conflated latest movement | The existing separate production latest-generation model and its negative control remain. |

The former child-only opaque-consumption model called the removed
`consume_opaque`. The product retired child subscriptions: `session.proto`
reserves the former `child` and `fs_publication` fields 11 and 12, and record
follows refuse a nonempty opaque resume. The replacement anchored-cut model
retains generic atomic movement and stale-cut controls; it does not claim to
qualify historical child resume. Historical source and receipts remain unchanged
in Git and retain their original scope.

Actor-call opaque bytes still describe immutable response length. Actor-call
fragment, reassembly, refusal, and cancellation assertions remain at their
existing owners; this mapping introduces no resumable actor-call cursor. Existing
source wake/recheck models remain distinct from these admission histories.
