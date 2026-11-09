# Local ready snapshot allocation

Run `node rust/proofs/qualify.mjs local-ready-snapshot-allocation`.

The existing public Local and Memory follow oracle reads actual records from
independently initialized providers, including a real Local journal, and checks
record contents, metadata, observer order and allocation counters. The retained
production mutation restores the boxed private Local snapshot helper. The
unchanged zero-allocation assertion must reject it before the baseline cost
assertion; both assertions are retained.

The private helper shares the original failure watch and publication lock with
all its callers. Keeping its future inline removes one allocation per snapshot
without changing erasure at the public provider boundary. It introduces no
task, syscall, copy, durability barrier or round trip. The existing
`local-failure-progress` observation control separately removes this helper's
failure observation and checks the original public waits before any further
await, so zero allocation cannot mask a lost producer.

This control qualifies ready reads through those public providers. It does not
establish that live native I/O will finish, bound scheduling latency or qualify
other platforms.
