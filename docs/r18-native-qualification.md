# R18 native sandbox qualification

The shell uses one `ExecutionGrant` core and native Linux Landlock/seccomp,
macOS Seatbelt and Windows PSEC adapters. Approval policy is independent of this
boundary. Native capability absence is a refusal, never permission to launch
without confinement.

## Failure modes

| Failure mode | Test or proof, on Linux, macOS and Windows |
| --- | --- |
| Unrelated files become readable or writable | `execution::tests::sandbox_policy`: ordinary-process control succeeds, confined absolute, `../outside` and workspace-symlink reads/writes receive PermissionDenied; host sentinel remains unchanged. |
| A descendant sheds its parent's sandbox | The same matrix launches the test executable through ordinary `std::process::Command`; the descendant runs the entire probe, including symlink/traversal denial, admitted workspace/volume operations, metadata, the real shell and TCP denial, without a second grant or adapter call. |
| Read-only primary or auxiliary grants regain writes | The same matrix covers writable primary/volume, read-only primary with writable volume, and both read-only, with and without process custody. |
| Required shell dependencies or workspace metadata are denied | The same matrix runs the real platform shell, reads the workspace/volume, stats every ancestor and enumerates `.cargo`; Linux also executes `ls -la && rg --files`. |
| A default confined process reaches a host TCP listener | The same matrix proves the unconfined connection first; confined connection is denied and the listener receives nothing. |
| Inherited file handles bypass pathname confinement | `execution::tests::inherited_file_stdio_cannot_bypass_the_workspace_boundary` rejects outside-file stdin and stdout with and without custody, preserves the sentinel and runs the identical unconfined shell as a positive control for each direction. |
| A host-composed read scope exceeds its retained parent | `execution::tests::scoped_native_reads_pin_the_original_parent_and_refuse_escape` rejects foreign scopes; Unix also checks symlink/replacement, Windows checks the original directory cannot be renamed while pinned. |
| A missing native enforcement capability is silently skipped | `sandbox_policy` unwraps the confined launch: no ignored test, capability skip or unconfined fallback. Windows requires native PSEC 1.1. |

Native stdio refusal preserves `PermissionDenied` or `Unsupported` without
equating typed preparation refusals with `Unsupported`.

Additional existing Unix proof `execution::tests::sandbox_denies_outside_write`
covers a workspace symlink to an outside file and an inheritable outside file
descriptor. These checks supplement the common matrix; they do not substitute
for native Windows execution.

## Native batch

Run the same three selected tests on Linux, macOS and Windows, from one signed
source and its native test executable (Windows binaries are cross-built).
Use the existing lifecycle runner to hold each execution until its target is
READY; target restoration and its lifecycle record have a separate owner.
Do not substitute cross-compilation or an unavailable capability for execution.

```sh
cargo test --locked -p acyclic-native-runtime --lib -- --exact \
  execution::tests::sandbox_policy \
  execution::tests::scoped_native_reads_pin_the_original_parent_and_refuse_escape \
  execution::tests::inherited_file_stdio_cannot_bypass_the_workspace_boundary \
  --nocapture
```

Each native result must report **3 passed, 0 failed, 0 ignored**. Retain the
source SHA, executable digest, native OS identity and terminal/cleanup receipt.
The Windows fixture requires symlink creation (Developer Mode or the test
account's native symlink privilege), as well as PSEC 1.1. Fixture failure is not
an ignored test. Linux requires `rg` in the test parent's search path. macOS uses
its installed native toolchain with `CARGO_INCREMENTAL=0` and the shared target
`/Users/var/foreman/target/lanes`.

## Evidence status

Completion requires the shared matrix and scoped-read test to execute on all
three native OSes from the same pushed source. Cross-compilation establishes
compilability only. The native Windows execution target and native macOS target
were unavailable in the current Foreman status at the start of this work.
No platform is recorded as qualified here until its exact-source terminal proof
is retained. Historical Linux/macOS passes do not qualify the changed matrix.
