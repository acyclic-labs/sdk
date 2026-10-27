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

`proofs.json` declares actual production mutations. The producer changes one
production file in a job-owned derivative and checks its complete tracked
input manifest before compiling; the tests and proof harnesses stay unchanged.
It records commands, toolchain, raw log hashes, timings, mutation hashes and
Cargo-selected executable hashes. `run.py` requires canonical bb2 admission,
uses the existing compiler cache and caps each job's command budget at 500 s.
Never invoke it as a local test or rerun an identical completed producer.

Run the `rust`, `kani`, `mutants` and `finish` modes through canonical bb2
`build-slot`, with `BUILD_SLOT_COMPILER=1`, SDK tenant `graphcoder`, PR 1697,
and the actual source base in the complete input declaration. Each invocation
needs a fresh `--evidence` directory. `remaining` resumes only the compiler
check and missing-method control after already-captured successful doctests;
it does not replay the positive properties. Installed Kani uses
`KANI_HOME=/home/var/.kani` inside the protected job home.
