# Process-event registration retirement

`registration_cancellation_retires_original_waker_outside_registry` uses the public
`descriptor_readiness` entry with an original Unix socket and its real native
readiness producer. It cancels pending observations with both ordinary and
panicking waker destructors. The callback panic must remain the original payload,
must not poison the native registry, and must not retain the original source
claim. The same descriptor is admitted again after each cancellation.

Mutex poisoning observes the lock held by the panicking thread. The negative
control fails an assertion rather than waiting for a deadlock; unrelated native
producer threads cannot change that observation.

`original-registration.patch` is captured from the production diff. It restores
the complete original production file from
`080831aa99e64eaaf574d1ae8f9872fa1ce3eae3` while retaining the same property:

```sh
git apply rust/proofs/process-event-retirement/original-registration.patch
git diff --exit-code 080831aa99e64eaaf574d1ae8f9872fa1ce3eae3 -- rust/crates/native-runtime/src/process_tree/events/mod.rs
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(registration_cancellation_retires_original_waker_outside_registry)'
git apply -R rust/proofs/process-event-retirement/original-registration.patch
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib --no-fail-fast -E 'test(registration_cancellation_retires_original_waker_outside_registry)'
```

The original source must fail with `original registration destroyed its waker
under the registry lock`. A compilation error or absent test is not a qualified
negative.

Safety: removal and source-claim release remain under the original lifecycle
lock; the removed signal owner retires after that lock. Progress: cancellation
adds no wait and permits the original callback to re-enter admission. Cost:
one registry lock and the same indexed removal before and after; no added
allocation, scan, syscall, payload copy, or native notification. The retirement
ordering adds no work dependent on live or historical registrations.

The other `Registry::remove` callers already transfer removed signal ownership
to terminal delivery or source retirement outside the registry lock. This
control covers cancellation retirement; waker registration and worker admission
have separate callback boundaries.
