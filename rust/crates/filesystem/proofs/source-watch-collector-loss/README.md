# Shared source collector loss

The Linux control starts the production source-watch driver on a real root and
its grandchild. Both watches share the original inotify instance and ancestor
registrations. It invokes the collector's production terminal transition while
their read cursors are held. Notification must therefore complete without a
service pass acquiring either cursor. Later subscription and fence calls must
refuse with the retained cause. Physical stop must release the original resources
and return that same cause, rather than treating loss as cleanup acknowledgement.

The process-wide failure is isolated in a child running the same test binary.
There are no recorded wire fixtures or replacement OS descriptors. This control
qualifies the terminal transition and its consumers; it does not establish an
actual kernel read/poll failure, unwind coverage, a subscription race proof, or
cross-platform execution.

Run from the repository root:

```sh
cargo test --locked -p acyclic-fs --lib source_watch::inotify::tests::collector_loss_rejects_subscriptions_and_preserves_owned_cleanup -- --exact --nocapture
```

The retained patch restores the original production collector, keeping only the
test entry to its existing `fail_all` transition. Apply it and run the same
control. It must fail at `collector loss admitted a subscription without a
producer`; a compile failure or another assertion is not the negative control.
Reverse it before running restoration:

```sh
git apply rust/crates/filesystem/proofs/source-watch-collector-loss/original-collector.patch
git apply -R rust/crates/filesystem/proofs/source-watch-collector-loss/original-collector.patch
```

Subscriber tables use the existing dense index implementation so iteration
visits live entries rather than historical hash-table capacity. Keyed insertion
and swap removal retain their expected amortized cost. This control does not
measure allocation counts or prove a worst-case constant-time bound.
