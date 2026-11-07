# Darwin retirement refusal and callback custody

The existing `retirement_keeps_callback_storage_until_terminal_completion`
control includes the production C adapter and substitutes only the framework
boundary. It checks every SDK `DAReturn` class and an unknown status, across
0–1024 delayed callback deliveries, plus each allocation and source-installation
failure. Every submitted request completes before its storage or sources are
released; absent sources refuse before submission. Native refusal never proves
physical kernel detachment.

The same control passes every native result through the production Rust
conversion and checks both its I/O error kind and the downcast original
`DarwinRetirementFailure`. Allocation and source-installation refusals retain
their raw errno. The callback returns a Boolean, so assertions never unwind
across the C boundary.

On macOS, run:

```sh
cargo test --locked -p acyclic-fs --all-features retirement_keeps_callback_storage_until_terminal_completion -- --nocapture
```

Apply `flatten-dissent.patch` and repeat that command. It must fail the native
I/O error classification before restoring the production source. This negative
changes the production projection, not a parallel model.

Repeat with `flatten-rust-kind.patch` and `erase-rust-cause.patch` separately.
Each must fail the same control before restoring the production source. These
mutations change the actual Rust conversion used by the unmount adapter.

Classification is one fixed switch over the callback result. It adds no
syscalls, durability barriers, round trips, allocations, copies or waits.
The existing callback remains the sole completion event; classification neither
retries a request nor releases an authenticated destination fence. All nonzero
statuses, including `kDAReturnNotMounted`, remain errors. Callback custody and
work counts are checked by the same included production control.

Lost Disk Arbitration service delivery is a separate, open progress obligation:
bounded delayed delivery does not prove that a dropped producer completes the
wait. This control must not be cited as that proof.
