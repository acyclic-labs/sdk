# Shared source collector loss

The Linux control starts the production source-watch driver on a real root and
its grandchild. Both watches share the original inotify instance and ancestor
registrations. While their read cursors are held, a real filesystem notification
wakes the original collector's poll. A test fault then unwinds that thread. The
control joins the original thread, so its production exit guard must publish
loss without a service pass acquiring either cursor. A concurrent subscription
waits immediately before acquiring the routes lock until that join completes.
The control observes the production subscriber insertion itself, because a later
startup read can refuse even after an incorrectly admitted subscription.
Subscription and
fence calls must refuse with the retained cause. Physical stop must release the original resources
and return that same cause, rather than treating loss as cleanup acknowledgement.

The process-wide failure is isolated in a child running the same test binary.
There are no recorded wire fixtures or replacement OS descriptors. This control
qualifies original-thread unwind, the terminal transition and its consumers; it
does not establish an actual kernel read/poll failure, all concurrent
interleavings, or cross-platform execution.

Run from the repository root:

```sh
cargo test --locked -p acyclic-fs --lib source_watch::inotify::admission_tests::collector_loss_rejects_subscriptions_and_preserves_owned_cleanup -- --exact --nocapture
```

`original-collector.patch` restores the original production collector, retaining
the same test-only thread handle and unwind injection. Apply it and run the same
control. It must fail at `collector loss admitted a subscription without a
producer`; a compile failure or another assertion is not the negative control.
Reverse it before running restoration:

```sh
git apply rust/crates/filesystem/proofs/source-watch-collector-loss/original-collector.patch
git apply -R rust/crates/filesystem/proofs/source-watch-collector-loss/original-collector.patch
```

The original-production patch also removes the exit guard. Joining the unwound
thread remains finite, then the subscription assertion detects the missing loss
publication. This keeps one mutation and one property for that failure, without
a timeout or a second equivalent guard-omission check.

`check-before-lock.patch` moves the availability check before the routes lock.
Run the same control with that mutation and restore it afterward. The forced
overlap must fail the same admission assertion, even when later startup cleanup
withdraws the incorrect insertion. The boundary waits use channels whose senders
are owned by the same scoped invocation; early failure disconnects the waits
before joining the subscription thread. No readiness timer is involved.

Subscriber tables use the existing dense index implementation so iteration
visits live entries rather than historical hash-table capacity. Keyed insertion
and swap removal retain their expected amortized cost. This control does not
measure allocation counts or prove a worst-case constant-time bound.
