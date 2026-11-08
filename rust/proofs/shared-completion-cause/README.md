# Shared completion retains the original typed cause

Run `node rust/proofs/qualify.mjs native-custody` from the repository
root. The shared qualifier applies the retained production mutation, requires
the existing native task custody test to reject it with
`shared completion replaced the original typed cause`, restores the exact source
bytes, and requires the same test to pass.

The original control cancels a completion waiter, drops its observer and loses
the original release channel's producer. Surviving observers must share one
operation error retaining that channel's typed receive failure. Its later real
task panic must retain the typed join failure behind the shared error while
keeping its panic payload out of the public diagnostic.

The mutation rebuilds each task error once before sharing it. Observer identity,
error kinds, diagnostics and finite completion survive, but the underlying cause
becomes a string. Both operation errors and native join failures pass through
this same production mapping; the restored test exercises both branches. The
negative fails at the first cause check. This complements the separate mutation
that rebuilds errors independently for each observer.

No production operation, test entry point or scheduler is added. This control
qualifies the native cause-retention boundary only. Complete scheduler progress,
deterministic execution, other operating systems, deployment topology and
population-dependent costs remain separate obligations.
