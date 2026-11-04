# Persisted operation-window validation

`window_state` calls the same oracle as the fuzz package's
`persisted_window_records_preserve_fields_or_typed_corruption` property. Each
input first creates a valid Idle, Active or Reconciling state using
`StreamOperationWindowStore::compare_and_swap` over the real `MemoryStream`
backend. The oracle captures the committed record with the public `head` call.
It then mutates that record, appends it through a fresh backend and calls the
original public `load` entry point. No head metadata or provider response is
mocked, and no alternative window decoder is used.

The corpus contains mutation programs, not hand-written provider responses.
Their first byte selects one of sixteen cases:

| Byte modulo 16 | Input operation | Required outcome |
| --- | --- | --- |
| 0, 1, 2 | Unchanged Idle, Active, Reconciling record | Every field survives |
| 3, 4, 5 | Invalid version, foreign workspace, wrong revision | Typed corruption |
| 6, 7, 8 | Empty lease map, zero lease identity, mismatched map key | Typed corruption |
| 9, 10, 11 | Blank owner, zero expiry, zero reconciliation ticket | Typed corruption |
| 12 | Remaining input bytes replace the captured payload | Typed corruption or writer-admissible decoded state |
| 13 | Next two bytes select a little-endian position; fourth byte XORs its value | Same accepted-state/refusal invariant |
| 14 | Next two bytes select a little-endian truncation length | Same accepted-state/refusal invariant |
| 15 | Append under a child, leaving the actual implicit parent empty | Typed corruption |

For cases 12..14 the first byte's high nibble modulo three selects the original
Idle, Active or Reconciling state. Bit 6 omits `pending_parent`; bit 7 omits
`subsequent_parent`. Present pinned, pending and subsequent parents have distinct
identities, so field preservation detects swaps as well as dropped fields.
The optional-parent corpus programs select Active without a pending parent and
all three absent-parent combinations of Reconciling. Like the other programs,
they obtain persisted bytes from the public writer at execution time.
An arbitrary accepted state must be
admitted by the original writer on a fresh backend and preserve all fields on another public read. The deterministic
property replays the stored programs, all sixteen cases with all four optional
parent combinations, and every low-byte
position/truncation selector for all three phases. The fuzzer varies both bytes
of the selector, the entire payload and edit.
Inputs and execution are bounded by actual input/output bytes, with no history
scan or readiness polling.

Run the kept property or replay the actual fuzz corpus:

```sh
cargo test --locked -p acyclic-fs-fuzz --test window_state persisted_window_records_preserve_fields_or_typed_corruption -- --exact
cargo run --locked -p acyclic-fs-fuzz --features fuzz-targets --bin window_state -- rust/crates/filesystem/fuzz/corpus/window_state -runs=0
```

The patches in `mutations/window_state` remove the original owner or ticket
validation, or swap the actual reader's pending and subsequent parents. Apply
one at a time, run the SAME oracle, require its typed-corruption or original-field
assertion to fail, then reverse the patch. A compiler failure is not detection.
The source coverage report and mutation receipt must identify their exact
production source; corpus replay alone is not source branch coverage.

This oracle checks the production memory backend and persisted window boundary.
It does not qualify remote transport, malformed backend head metadata, physical
restart/upgrade, multi-member durability, caller cancellation or producer loss.
Existing conditional-transition, lease fencing and reconciliation controls are
kept for those separate obligations.
