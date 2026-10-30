# Agent boundary proofs

The Kani package includes the production slot arithmetic by path. Its four
harnesses quantify over all machine-sized counts, with a valid starting state
(`open <= 256`, `reserved <= 256 - open`) where needed. They establish the
client ceiling, room for each reserved child, preservation of a paused turn's
capacity when a child finishes, and overflow-safe replay admission. They do
not assume a second implementation of the arithmetic.

`agent-model::slot_schedules` executes the production serialized actor step:
60 pause/client/resume schedules, client response-loss replay, and completion
of an already-open recorded child during an approval pause. Production actor
steps commit serially; this is a deterministic transition simulation rather
than a model of different concurrency primitives. It does not qualify a
provider's crash recovery or hosted endpoint.

The completion regression found a lost reservation: a recorded spawn can
already name an open child, so it initially reserves no additional capacity.
If that child finishes while the turn pauses, a client could take the vacated
slot before the recorded spawn settles. The transition now holds vacated
slots until the driver reconciles its recorded effects. Duplicate completion
does not reserve twice. This conservatively holds other vacated slots during
the active turn as well; settlement releases them.

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
