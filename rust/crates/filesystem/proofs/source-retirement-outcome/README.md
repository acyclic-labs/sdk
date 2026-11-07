# Retained source retirement outcome

The Linux property constructs a real native demand source and production
`LazyMountSource::new`, with the original change recorder and native watch. A
lookup reaches a file two directories below the root. The fixture's existing
memory authority only supplies the authored checkout; it does not replace the
native subscription, OS notification or retirement implementation.

While the actual watch read cursor is held, authored cleanup completes and its
caller abandons the native wait through the real cancellation registration.
After cursor release, a real file write invokes the original recorder, which
refuses because authored capture was cancelled. The driver retires its original
resources and signals its terminal publication. A retry with the same cancelled
caller must consume that saved failure once, report no additional physical
transition, and leave the original authored work receipt unchanged.

```sh
cargo test --locked -p acyclic-fs --lib native_mount::lazy::tests::source_changes::cleanup_retry_observes_the_retired_source_watch_failure -- --exact --nocapture
```

`retired-lazy-shortcut.patch` restores the production early return that erased the
saved outcome. Run the test while the mutation is applied, then restore it:

```sh
patch=rust/crates/filesystem/proofs/source-retirement-outcome/retired-lazy-shortcut.patch
git apply "$patch"
trap 'git apply -R "$patch"' EXIT
cargo test --locked -p acyclic-fs --lib native_mount::lazy::tests::source_changes::cleanup_retry_observes_the_retired_source_watch_failure -- --exact --nocapture
git apply -R "$patch"
trap - EXIT
cargo test --locked -p acyclic-fs --lib native_mount::lazy::tests::source_changes::cleanup_retry_observes_the_retired_source_watch_failure -- --exact --nocapture
```

The mutation must fail at `cleanup retry erased the retired watch's callback
failure`; restoration must pass. Compilation failure and unrelated assertions
are not negative evidence. These commands intentionally continue after the
expected failing invocation; the EXIT trap also restores production on early exit.

Completed retries now traverse the existing watch outcome consumer instead of
returning before it. They add no syscall, allocation, durability barrier, copy or
round trip. The fixed number of original watch/controller locks replaces the
incorrect zero-work shortcut. This control covers the forced real-driver
interleaving, not all platform schedules or mounted-kernel end-to-end behavior.
