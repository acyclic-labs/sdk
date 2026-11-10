# Agent boundary proofs

The former Kani slot arithmetic is retired: child count and depth are no longer
admission conditions. Historical receipts remain available and do not qualify
the new persistent membership index. Its production tests compare paged point
updates and reopen against membership, with bounded node-read/write counters.

`agent-model::slot_schedules` retains serialized pause/client/resume schedules,
response-loss replay, and child completion during approval pauses. The native
reopen controls retain published membership roots and the driver's checkpoint.

The generated `ExplicitStreamProvider` removes defaults from the canonical
provider declaration. Missing adapter implementations fail compilation,
including future default methods. The property exercises every currently
supported attested write family and the prepared-commit refusal. Its internal
probe checks the canonical digest at the bound provider and captures original
local/admitted authority on exact retries. This verifies adapter forwarding;
an external `Attestor` must still satisfy its documented deterministic-function
contract. The admitted-authority probe is not a deployed Cloud receipt.

The record property includes every agent inbox/status/closed/conversation
variant, maximal field lengths and collection cardinalities, the deepest
paths, largest counters, every outcome, and maximal JSON escape expansion.
It checks canonical bytes before the bounded encoder's refusal. An oversized
record refused by the encoder therefore fails the property. The settled
checkpoint and actor envelope bound retain their separate maximum-size test.

`rust/proofs/spawn-slots/proofs.json` declares actual production mutations. The producer changes one
production file in a job-owned derivative and checks its complete tracked
input manifest before compiling; the tests and proof harnesses stay unchanged.
It records commands, toolchain, raw output hashes, timings, mutation hashes and Cargo-selected executable hashes.

Install the pinned Rust toolchain and Kani. Run `python3 rust/proofs/spawn-slots/run.py MODE --evidence /path/to/new/output` from a clean checkout, selecting `rust`, `kani`, `mutants` or `finish`. Each invocation requires a fresh evidence directory. Configure Kani through its standard environment or PATH. `remaining` resumes only the compiler check and missing-method control after the captured successful doctests; it does not replay the positive properties.

Main's correlated-turn revision adds WaitingHost. Its host must satisfy the same
1–128 byte, non-control name bound as inbox host names. The transition turns an
invalid driver wait into a bounded failure before publishing its effects. The
maximum-record property includes the new phase and conversation inbox sequence.

The versioned admission manifest
retains their SHA256 hashes and the original captured archive hashes.
