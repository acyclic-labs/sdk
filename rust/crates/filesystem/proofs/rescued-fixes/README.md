# Rescued filesystem regressions

Run `bash rust/crates/filesystem/proofs/rescued-fixes/check.sh` on a disposable
Linux checkout. The script records its source SHA, checks the filesystem crate,
runs the affected tests, then removes seven protections in one mutant build.
Every original regression must fail at runtime; compilation errors do not count.
The checkout is restored on exit.

| Rescue | Regression and source control |
| --- | --- |
| be-host-parser-validation | Derive invalid typed writer inputs from admitted captures; omit leaf ordering. |
| co-green-2 | Share the real operation cache under source cursor locks; lower its lock rank. The same test runs under Loom when compiled with `cfg(loom)`. |
| co-green | Reopen while physical ownership is retiring; omit native claim handles. The test waits for the claim's actual waiter-registration event. |
| co-quality-fs | Exhaust a narrow index domain against a wide minimum oracle; truncate allowances to that domain. This reproduces the conversion error on a 64-bit test host. Real metered writes are also checked. |
| comp | Register and recover stored Unicode identities; accept normalized replacements. |
| fs-scale-control-original-work-20261108 | Activate a real source with foreign work; omit original-invocation admission. |
| fs-scale-main-remaining | Publish a history of bindings and revisions; omit publication admission. Additional patches independently remove each binding/revision predicate. |

No live mount is needed for these regressions. Physical Windows and macOS
adapter execution and exhaustive Loom schedules require their existing platform
and model qualification; the ordinary callback test checks the same code path.
