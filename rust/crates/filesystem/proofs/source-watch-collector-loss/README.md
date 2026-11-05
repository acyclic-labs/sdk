# Shared source collector loss

The Linux control starts the production source-watch driver on a real root and
its grandchild. Both watches share the original inotify instance and ancestor
registrations. While their read cursors are held, a real filesystem notification
wakes the original collector's poll. A test fault then unwinds that thread. The
control joins the original thread, so its production exit guard must publish
loss without a service pass acquiring either cursor. Later subscription and
fence calls must refuse with the retained cause. Physical stop must release the original resources
and return that same cause, rather than treating loss as cleanup acknowledgement.

The process-wide failure is isolated in a child running the same test binary.
There are no recorded wire fixtures or replacement OS descriptors. This control
qualifies original-thread unwind, the terminal transition and its consumers; it
does not establish an actual kernel read/poll failure, a subscription race proof,
or cross-platform execution.

Run from the repository root:

```sh
cargo test --locked -p acyclic-fs --lib source_watch::inotify::tests::collector_loss_rejects_subscriptions_and_preserves_owned_cleanup -- --exact --nocapture
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

`omit-exit-guard.patch` removes only the production exit guard. The same control
must fail at the same subscription assertion after joining the unwound thread;
it cannot rely on a timeout to detect the missing loss publication. Reverse the
patch before running restoration. Both controls keep the real driver startup,
notification, thread entry and cleanup paths.

Subscriber tables use the existing dense index implementation so iteration
visits live entries rather than historical hash-table capacity. Keyed insertion
and swap removal retain their expected amortized cost. This control does not
measure allocation counts or prove a worst-case constant-time bound.
