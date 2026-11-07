# Abandoned prepared commit retires its original worker

Run `node rust/proofs/qualify.mjs local-prepared-ingress-close`.

The existing public Local prepared-packet abandonment oracle uses the real
journal and default storage configuration, with two immutable forks. The
existing native scheduler controls the actual accepted worker future. One
schedule drops its packet before the worker first executes; the other first
executes the worker until it parks on its original record ingress. Dropping
the packet must close that ingress and reach the original worker completion
boundary in the next owned-task step. The oracle checks that boundary before
awaiting anything else, so a missing producer close fails deterministically.
It then releases the completion boundary, checks the original task drain and
unchanged tails, and verifies provider retirement and funding release.

Two retained mutations independently remove the production prepared-packet
destructor's ingress close or the original worker's wake registration. Missing
close is caught before worker execution; missing registration is caught only
after the worker parks. Each must fail at the same completion-boundary assertion.
The qualifier restores production before running the unchanged oracle again.
No timeout, polling loop, replacement producer or private receiver is used.
The scheduler polls the original shipped worker and its ingress state machine;
there is no parallel model of either.

This strengthens one existing control rather than adding another example test.
Production work is unchanged: zero added tasks, events, allocations, copies,
syscalls, durability barriers, round trips, locks or scans. Live storage that
never completes and arbitrary live-executor fairness remain separate obligations.
