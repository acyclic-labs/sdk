# Original observer registration

The native completion, controlled terminal, operation fence and process signal
use `wake::poll_registered` for their single observer slot. The terminal check
and final installation share the original state lock. Incoming waker cloning
and replaced-waker destruction run outside that lock. Unchanged-waker polls
retain their original one-lock, zero-clone path.

`original_driver_replacement_preserves_reentrant_publication_and_unwind`
replaces an observer through public `NativeCompletion::from_driver`. The
retiring observer checks the original lock and publishes through the original
producer, both with ordinary retirement and with its own callback panic. The
replacement must receive that publication, preserve the callback panic and
release the original allocation after its final owner retires.

The retained inverse is shared with completion-observer retirement and captured
from the production diff. It restores the
complete six documented files from
`a8643c4b2a447b800b215d939d084ec509652b09`, including the original callers in the
existing terminal-observation controls, while retaining the new property:

```sh
git apply rust/proofs/native-waker-registration/original-registration.patch
git diff --exit-code a8643c4b2a447b800b215d939d084ec509652b09 -- rust/crates/native-runtime/src/wake.rs rust/crates/native-runtime/src/completion/state.rs rust/crates/native-runtime/src/completion/mod.rs rust/crates/native-runtime/src/completion/fence.rs rust/crates/native-runtime/src/process_tree/events/signal.rs rust/crates/native-runtime/src/workers/job_observation_tests.rs
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(original_driver_replacement_preserves_reentrant_publication_and_unwind)'
git apply -R rust/proofs/native-waker-registration/original-registration.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(original_driver_replacement_preserves_reentrant_publication_and_unwind)'
```

The original must fail with `original observer replacement retained the
completion lock`. Compilation failure cannot qualify the negative control.

The existing `process_signal_loom` check includes the production signal and
the production wake module. It joins the original terminal producer before
checking that a pending poll received its wake, so a lost wake is an assertion
failure without a timeout or a stranded waiter. It also retains the first
terminal outcome under competing failure and cancellation. Run the instrumented
production code with:

```sh
node scripts/qualify-mcp-loom.mjs --native-signal
```

`missing-publication-recheck.patch` is captured from a real production mutation
that removes only the post-clone terminal check. Its diff target headers are
normalized to the production file. Applying it must make
`settlement_cannot_pass_waker_registration` fail with `original terminal passed
observer registration without a wake`:

```sh
node scripts/qualify-mcp-loom.mjs --native-signal-mutations
```

The shared qualifier plants this exact mutation, checks the unchanged oracle,
restores the original source in `finally` and runs both restored signal models.
It compiles only the selected model target with `cfg(loom)`, leaving registry
dependencies in their ordinary production mode.

The helper has no loop, scan, wait, allocation, payload copy, syscall,
durability barrier or round trip. A terminal read or unchanged observer takes
one lock. A new or replaced observer takes two locks instead of one: the
second checks publication after arbitrary clone callbacks and retains exactly
one incoming clone. Fence and controlled-terminal unchanged-waker clones fall
from one to zero; completion and signal unchanged-waker clones remain zero.
These are bounded core bookkeeping counts, independent of history or agents;
they do not bound user callback work or certify worker-queue admission.

Intrusive capacity admission and cancelled fence-chain retirement have distinct
ownership paths and remain outside this registration qualification.
