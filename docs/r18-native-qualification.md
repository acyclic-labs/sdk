# R18 native sandbox qualification

The shell uses one `ExecutionGrant` core and native Linux Landlock/seccomp,
macOS Seatbelt and Windows PSEC adapters. Approval policy is independent of this
boundary. Native capability absence is a refusal, never permission to launch
without confinement.

## Failure modes

| Failure mode | Test or proof, on Linux, macOS and Windows |
| --- | --- |
| Unrelated files become readable or writable | `execution::tests::sandbox_policy`: ordinary-process control succeeds, confined absolute and `../outside` reads/writes receive PermissionDenied; host sentinel remains unchanged. |
| A descendant sheds its parent's sandbox | The same matrix launches the test executable through ordinary `std::process::Command`; the descendant repeats absolute/traversal denial checks without a second grant or adapter call. |
| Read-only primary or auxiliary grants regain writes | The same matrix covers writable primary/volume, read-only primary with writable volume, and both read-only, with and without process custody. |
| Required shell dependencies or workspace metadata are denied | The same matrix runs the real platform shell, reads the workspace/volume, stats its parent and enumerates `.cargo`; Linux also executes `ls -la && rg --files`. |
| A default confined process reaches a host TCP listener | The same matrix proves the unconfined connection first; confined connection is denied and the listener receives nothing. |
| A host-composed read scope exceeds its retained parent | `execution::tests::scoped_native_reads_pin_the_original_parent_and_refuse_escape` rejects foreign scopes; Unix also checks symlink/replacement, Windows checks the original directory cannot be renamed while pinned. |
| A missing native enforcement capability is silently skipped | `sandbox_policy` unwraps the confined launch: no ignored test, capability skip or unconfined fallback. Windows requires native PSEC 1.1. |

Additional existing Unix proof `execution::tests::sandbox_denies_outside_write`
covers a workspace symlink to an outside file and an inheritable outside file
descriptor. These checks supplement the common matrix; they do not substitute
for native Windows execution.

## Evidence status

Completion requires the shared matrix and scoped-read test to execute on all
three native OSes from the same pushed source. Cross-compilation establishes
compilability only. The native Windows execution target and native macOS target
were unavailable in the current Foreman status at the start of this work.
No platform is recorded as qualified here until its exact-source terminal proof
is retained. Historical Linux/macOS passes do not qualify the changed matrix.
