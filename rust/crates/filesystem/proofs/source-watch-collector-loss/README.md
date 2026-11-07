# Shared source collector loss

The Linux property starts production source watches on a real root and its
child/grandchild. They share the original inotify instance and ancestor
registrations. While two read cursors remain held, a real filesystem notification
wakes the original collector. A fault unwinds that thread; its production exit
guard must publish loss without acquiring those cursors. A concurrent
subscription pauses immediately before the routes lock until the collector joins.
The property observes insertion itself, because later startup cleanup can refuse
an incorrectly admitted subscription.

A third subscription stops while its failure observer is paused. Cleanup must
already return the saved collector cause, exactly once, even though notification
has not returned. Outcome publication shares the existing subscription/withdrawal
lock. Strong watch custody survives that lock so final destruction and observer
callbacks can withdraw without recursive locking. Physical retirement still owns
its original descriptors and cleanup credit.

The permanent process-wide failure is isolated in a child of the same test binary.
Inputs are actual filesystem notifications, with no replacement descriptors or
recorded provider fixtures. This covers original-thread unwind and the forced
subscription/cleanup interleavings; it does not establish kernel poll failure,
all interleavings, or other operating systems.

Run from the repository root:

```sh
cargo test --locked -p acyclic-fs --lib source_watch::inotify::admission_tests::collector_loss_rejects_subscriptions_and_preserves_owned_cleanup -- --exact --nocapture
```

The retained mutations each change the production implementation:

| Patch | Required failing assertion |
| --- | --- |
| `original-collector.patch` removes the actual exit guard | `collector ended without publishing loss` |
| `check-before-lock.patch` moves admission before subscription custody | `collector loss admitted a subscription without a producer` |
| `publication-after-notification.patch` restores reporting before retaining the cause | `cleanup erased collector loss during notification` |

For each patch, run the property between application and reversal. For example:

```sh
patch=rust/crates/filesystem/proofs/source-watch-collector-loss/publication-after-notification.patch
git apply "$patch"
trap 'git apply -R "$patch"' EXIT
cargo test --locked -p acyclic-fs --lib source_watch::inotify::admission_tests::collector_loss_rejects_subscriptions_and_preserves_owned_cleanup -- --exact --nocapture
git apply -R "$patch"
trap - EXIT
cargo test --locked -p acyclic-fs --lib source_watch::inotify::admission_tests::collector_loss_rejects_subscriptions_and_preserves_owned_cleanup -- --exact --nocapture
```

The mutated run must fail at its listed assertion, then restoration must pass.
Compile failure, no selected test, and any other assertion are rejected evidence.
If the exit guard is absent, joining the collector sends a completion event to the
same observer channel, so the missing-publication control fails without a timer.
Scoped channel senders disconnect before scoped joins on an early failure.

Subscriber iteration visits current dense entries rather than historical table
capacity. Collector termination necessarily visits its live subscriptions; each
subscription/withdrawal remains keyed. The terminal path adds one vector allocation
holding one Arc per registered subscription (no growth reallocations), one second
error wrapper per live subscription, and one failure-slot lock per live
subscription. It adds no syscall, durability barrier, round trip or healthy-path
work. This property does not prove worst-case constant time or allocation counts.
