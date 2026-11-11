# R18 gaps on main

Baseline: `e4559fea6a2e4ba3c8ff83612a2b33ff58dc8a42` (2026-11-11).
This is an implementation audit, not enforcement proof.

- All OSes: the default shell grant enables networking (`rust/crates/harness-core/src/filesystem/native_process/shell.rs:197`). Replace this with a shared policy that denies access outside the execution boundary; keep approvals orthogonal.
- Linux: the default grants read access to `/` (`shell.rs:153` in the same directory), and the native test repeats that broad grant (`rust/crates/native-runtime/src/execution/tests.rs:155`). Grant only the composed workspace, visible volumes and explicit runtime dependencies; prove an unrelated file cannot be read or written.
- macOS: inherits the same `/` default. Seatbelt additionally allows global file metadata queries (`rust/crates/native-runtime/src/execution/macos.rs:159`), independently of the roots in the core. Scope filesystem metadata to the same roots, retaining only documented native runtime necessities.
- Windows: defaults read every available drive (`rust/crates/harness-core/src/filesystem/native_process/shell.rs:156`; `rust/crates/native-runtime/src/execution/windows.rs:173`). PSEC unconditionally receives all network capabilities and allow ingress/egress (`execution/windows/specification.rs:48`). Restricted network grants currently refuse (`execution/windows.rs:80`); represent the default network boundary natively without claiming the stronger socket-free `Off` contract.
- Shared proof: Windows has a separate write-only test (`execution/tests.rs:25`), Unix has another (`execution/tests.rs:117`), and network enforcement runs only on Linux (`execution/tests.rs:1034`). Run one real-process read/write/network policy matrix on all three OSes, with admitted controls and required native capability failures treated as failures.

Paths abbreviated as `execution/` above are under `rust/crates/native-runtime/src/`.

R27 coordination boundary: `swarm-modes` owns optional fusectl, capability probing and child merge-back in mounting modes (#3423). Reuse those changes; this task owns the stock-container qualification and user-facing no-FUSE documentation. Main already has `scripts/qualify-docker-no-fuse.mjs`; inspect its exact-source evidence before changing or repeating it.
