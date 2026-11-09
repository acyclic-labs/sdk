# Configuration composition evidence

Run `node rust/proofs/qualify.mjs config-composition-cut`.

The existing generated evaluator oracle calls `config::compose_with_evidence`
with a successful seed, an absent middle layer and a missing dependency in
the next module. Its original typed refusal must keep both attempted module
cuts and the absent dependency. A later layer must remain unevaluated.
Changing only the absent layer's generation must change the decision identity.
A higher `off` marker must suppress even invalid lower source, retaining the
base and the original ordered observations. The existing composition oracle
also checks successful callback results against independent values and the
original module provenance.

The first mutation drops the original refused module's dependency observations.
The second drops ordered layer generations from the composition identity.
Both must fail the unchanged oracle; restored source must pass it.

Both public composition entry points use one fold and the one layer selector.
Existing `compose` callers allocate zero new composition evidence buffers.
The evidence entry point adds one erased future allocation, one layer metadata
buffer (up to two owned metadata strings per supplied layer) and the attempted
module-cut buffer. It moves the loader's read sets instead of cloning them or
walking dependencies again. Additional tree reads, source-byte copies, VM
entries, syscalls, round trips and durability barriers are all zero. An empty
fold canonicalizes its base once to bind it; a nonempty fold reuses the original
evaluation keys that already bind their bases. Hashing and retaining evidence
scale with that original cut, without retaining a history of calls. Borrowing
a retained result or reading its identity does no I/O or evaluation.

Operational read failures remain typed outer errors. This change adds no wait
or readiness loop. It does not authenticate callers, recheck current policy,
bind host selection or publish receipts; those remain with their original
owners. Its identity describes inputs and must never authorize recovery.
