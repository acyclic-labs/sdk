# Persisted socket ledger captures

Source: `0a9b0dc8299ad5ad99baac9c2b34f38bb4226622`, tree
`9f6963de33ac7a6388d898d80adf62c2bfe57699`.
Producer: `cargo test --locked -p acyclic-actors --lib local::sockets::tests::socket_conformance_passes_on_local_runtime -- --exact --nocapture`
(Linux x86_64, 1/1 passed, 0.17 s).

The original `epoch_record` and `ledger_record` writer boundaries retained their
input fields and emitted bytes while the shared conformance suite used actual
LocalStream storage, Runtime, socket sessions and restart. Each selected writer
projection was joined by exact bytes to records read from the epochs stream and
its two committed ledger segments after that suite. Observation was removed
before decoder validation; no fixture bytes or expected fields were reconstructed.

`socket-ledger.capture` retains the five original projections: initial epoch,
recovery fence, recovered epoch, opened connection and closed connection.
Original producer source SHA-256:
`ee05f91d0f5239b62dfd0f01ca7479e2d13bf3927184ddf1fbae2f7f3dd372e5`;
original test source SHA-256:
`ee55a35ef70aee5564c9c8b875d7e87e24a8e7af1240794d537f1c0ff19594ea`.

The same `actor_codecs` target requires admission of every retained projection
and compares decoded fields to the original writer inputs. Additional boundary
values come from the actual shared epoch writer; an independent `u128` oracle
checks format capacity and frontier validity independently: an exhausted segment
with a zero frontier isolates capacity refusal. These are derived inputs, distinct
from the original session captures. Unknown-field and version refusals are
also derived from original bytes.

These codec checks do not qualify the entire ledger fold, interrupted compaction,
foreign members, upgrades or lost acknowledgements. Native counter-exhaustion
controls and physical session conformance have separate obligations.
