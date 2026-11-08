# Original native completion callback custody

A consumer Waker, completed-write meter or final allocation refund can unwind
on the native producer. The original producer retains its typed panic through
the existing task-error mechanism. Active and late observers retain the same
terminal cause. Notification failure cannot unwind through the Windows
thread-pool callback's C ABI.

Windows must also retire kernel-owned operation references. Dequeued packets
use the shared terminal delivery primitive, including final Arc destruction.
A completed write publishes its result and clears pending even when its meter
unwinds. After a consumer failure, the original IOCP producer keeps draining
actual completion packets; new transfers reject the retained service failure.
No second registry, counter, controller or replacement producer is introduced.
An OS queue failure remains a distinct native failure, not a successful drain.

Normal-path syscalls, durability barriers, round trips, allocations and payload
copies are unchanged. Each transfer adds one retained-failure check; each
native batch and completed write adds one unwind boundary. Work is O(1) per
operation or delivered observer, with O(N) delivery for N actual completions.
The fixed producer count and original OS wait remain unchanged. Outstanding
operations retain their storage until their actual terminal packet; their
original owners still cancel or close their native handles.

The census is
`rg -n 'fn run|fn complete|finish_write|Arc::from_raw|post_exit' rust/crates/native-runtime/src/process_tree/events`.
The Unix control uses real socket readiness, production registration and the
original native producer/dispatch. A consumer panic must leave the original
typed payload retained and settle an independent quiet source and late observer
with the same cause. It has no timed probe or retry. The patch restores the
complete producer file, including registration retirement, from `31f178b42d19355775cf845dcb023f2824252f88`.

```sh
git apply rust/proofs/native-completion-callbacks/original-producer-unwind.patch
git diff --exit-code 31f178b42d19355775cf845dcb023f2824252f88 -- rust/crates/native-runtime/src/process_tree/events/mod.rs
# Require runtime failure: native producer lost typed panic custody.
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib -E 'test(=process_tree::events::tests::native_producer_retains_original_consumer_panic)'
git apply -R rust/proofs/native-completion-callbacks/original-producer-unwind.patch
# Require the same property to pass after restoration.
cargo nextest run --locked -p acyclic-native-runtime --all-features --lib -E 'test(=process_tree::events::tests::native_producer_retains_original_consumer_panic)'
```

On Windows, the existing `rejected_exit_post_settles_active_and_late_waits`
control uses a real refused native post and now includes a panicking consumer.
All observers must retain the original OS error and the callback must return.
This is separate from complete packet-drain qualification. Real overlapped
write-meter unwind, multi-packet final-owner unwind and post-failure cancellation
controls remain required before claiming native Windows completion custody.
