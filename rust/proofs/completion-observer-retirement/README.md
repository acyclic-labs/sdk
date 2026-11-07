# Original completion observer retirement

`cancelled_original_driver_retires_observer_outside_completion_lock` creates the
measured public `NativeCompletion::from_driver` pair, registers its observer,
and drops that observer with ordinary and panicking waker destructors. The
original callback panic must survive without poisoning the completion mutex.
The original producer must retain the completion allocation until publication
and final shared-owner retirement.

The poison observation is specific to the panicking thread's lock custody;
it does not wait on a possibly held lock or race another thread's `try_lock`.

`original-observer.patch` is captured from the production diff and restores the
complete original completion file from
`510d0335cfff2d8b7f151d433f5b00d962cfc2bf`, retaining the same property:

```sh
git apply rust/proofs/completion-observer-retirement/original-observer.patch
git diff --exit-code 510d0335cfff2d8b7f151d433f5b00d962cfc2bf -- rust/crates/native-runtime/src/completion/mod.rs
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(cancelled_original_driver_retires_observer_outside_completion_lock)'
git apply -R rust/proofs/completion-observer-retirement/original-observer.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(cancelled_original_driver_retires_observer_outside_completion_lock)'
```

The original must fail with `original observer was destroyed under the
completion lock`. Compilation failure or an absent control does not qualify.

The fix moves the stored waker out of the completion mutex before destroying
it. Result and allocation ownership stay with the original shared state.
Cancellation adds no wait or readiness mechanism. Costs stay at one lock, one
slot clear and one waker retirement; there are no added allocations, copies,
syscalls, durability barriers, round trips or scans.
