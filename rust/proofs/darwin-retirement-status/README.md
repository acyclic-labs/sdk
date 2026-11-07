# Darwin retirement refusal and callback custody

The existing `retirement_keeps_callback_storage_until_terminal_completion`
control includes the production C adapter and substitutes only the framework
boundary. It checks every SDK `DAReturn` class and an unknown status, across
0–1024 delayed callback deliveries, plus each allocation and source-installation
failure. Every submitted request completes before its storage or sources are
released; absent sources refuse before submission. Native refusal never proves
physical kernel detachment.

On macOS, run:

```sh
cargo test --locked -p acyclic-fs --all-features retirement_keeps_callback_storage_until_terminal_completion -- --nocapture
```

Apply `flatten-dissent.patch` and repeat that command. It must fail the native
I/O error classification before restoring the production source. This negative
changes the production projection, not a parallel model.

Classification is one fixed switch over the callback result. It adds no
syscalls, durability barriers, round trips, allocations, copies or waits.
The existing callback remains the sole completion event; classification neither
retries a request nor releases an authenticated destination fence. All nonzero
statuses, including `kDAReturnNotMounted`, remain errors. Callback custody and
work counts are checked by the same included production control.