`native_receipt` calls the public `host_process::Receipt::{accepted,confirm}`
used by native process admission. Its oracle shares `AgentPath`'s parser and
checks only the receipt contract: confirmation retains the admitted task,
refuses another task before reading its view, and returns the current view
for the original task. Arbitrary JSON and sequences exercise failed admission,
missing and malformed fields, foreign identities and view remounts.

The same oracle is used by `tests/native_receipt.rs`, which creates an actual
native workspace owner, two sibling sessions and a nested child, retires the
owner and reopens its persisted sessions. It prints the original serialized
reply sequence as `NATIVE_RECEIPT_CAPTURE`; those SDK-produced bytes supply the
fuzzer corpus. Derived field mutations are not captured external responses.
This checks receipt admission, not native CLI execution, distributed serving,
process cancellation or the physical execution fence.

Run the producer control with
`cargo test -p acyclic-harness --no-default-features --features filesystem-local,native-execution --test native_receipt -- --nocapture`.
Run the retained target with
`cargo fuzz run native_receipt --fuzz-dir rust/crates/harness/fuzz-native-journal`.
Coverage must include the called harness library; instrumenting only this
target does not establish production receipt coverage. A planted removal of
the task comparison in `Receipt::confirm` must fail the unchanged oracle with
`foreign task passed native receipt confirmation`.
