# Cancelled file-operation observer retirement

A cancelled pending file operation must publish its original completion link
before retiring its predecessor's observer. `complete_after` takes the observer
and replaced skipped owner, updates the existing link, unlocks, then retires
those owners. Callback unwind therefore neither poisons the predecessor lock
nor loses the live successor's completion event.

`cancelled_file_successor_links_before_observer_retirement` uses public
`NativeFile::control_async` and `set_len_async` with the configured workers.
The original control body waits on its own gate. A second file operation
registers the original observer and a third follows it. Cancelling the second
must retire the observer outside the original predecessor lock, preserving an
ordinary return or its original destructor panic. The property checks the
original link before releasing the first operation, then completes the same
third operation and reads its actual file length. Its gate owner releases on
unwind, so an assertion failure cannot strand a worker.

The retained inverse restores the complete production fence implementation and
its shared registration dependencies
from `ef364fd00a3973e2d7afaff1fc981fccc4c3e941`, keeping that same property:

```sh
git apply rust/proofs/cancelled-fence-retirement/original-fence.patch
git diff --exit-code ef364fd00a3973e2d7afaff1fc981fccc4c3e941 -- rust/crates/native-runtime/src/wake.rs rust/crates/native-runtime/src/completion/state.rs rust/crates/native-runtime/src/completion/mod.rs rust/crates/native-runtime/src/completion/fence.rs rust/crates/native-runtime/src/process_tree/events/signal.rs rust/crates/native-runtime/src/workers/job_observation_tests.rs
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib -E 'test(cancelled_file_successor_links_before_observer_retirement)'
git apply -R rust/proofs/cancelled-fence-retirement/original-fence.patch
```

The original must fail with `cancelled file observer retired under predecessor
lock`. The separate `missing-original-link.patch` removes the actual producer's
link publication; the same property must fail with `cancelled file successor
lost its original completion link` before attempting its successor wait:

```sh
git apply rust/proofs/cancelled-fence-retirement/missing-original-link.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib -E 'test(cancelled_file_successor_links_before_observer_retirement)'
git apply -R rust/proofs/cancelled-fence-retirement/missing-original-link.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib -E 'test(cancelled_file_successor_links_before_observer_retirement)'
```

Compilation failure cannot qualify either negative. Production work stays at
one lock, one Arc increment for the existing link, one observer retirement,
and one replaced-owner retirement. Added allocations, syscalls, durability
barriers, round trips and payload copies are zero. Taking and replacing these
slots is constant work; original completion of a cancelled chain still visits
each affected link once and is not claimed to be constant for the whole chain.
