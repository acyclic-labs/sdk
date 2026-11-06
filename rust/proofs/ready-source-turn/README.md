# Bounded provider turns

`ReadySources::poll_next` consumes at most one wake hint before polling at most
one provider. Before parking it consumes at most one further hint and registers
its parent under that same signal lock. A buffered hint or an enabled runnable
source funds the next cooperative turn; otherwise the original provider callback
publishes the next event. The existing generation fence rejects stale callbacks.

The existing `ready_records_reuse_the_registered_source_node` control retains
32 actual records produced by `MemoryStream` append/read, their original order,
sequence, equality and zero-allocation observations. Retained cancelled callbacks
publish a burst of 32 hints. Each ready delivery must consume exactly one hint,
independent of that backlog. Another 32 actual follows park at the original tail;
each owner turn polls exactly one provider, then an actual append resumes them.
The original late-cancellation schedule is part of this same control: publication
after a stale callback must still wake the parent and return its committed record.

Run `python3 rust/proofs/qualify.py ready-source-turn`. The unchanged control must
reject four retained mutations of the actual scheduler, then pass after exact
source restoration:

- `mutation.patch` reinstates the original unbounded wake drain; the physical
  signal backlog assertion must fail.
- `../ready-source-pending/mutation.patch` polls the entire runnable queue before
  yielding; the actual provider-poll count must fail.
- `../ready-source-parking/mutation.patch` omits atomic parent registration;
  the original late-cancellation publication must fail its parent-wake assertion.
- `../ready-source-delivery/mutation.patch` drops a ready result; the same real
  committed-record assertion must fail.

There are no added threads, timers, syscalls, durability barriers, round trips,
payload copies or per-delivery allocations. A ready record uses the original one
signal lock and one provider poll. Parking uses at most two signal locks and one
provider poll, instead of draining the backlog and polling the runnable population.
The one extra wake names already buffered or runnable work; it is not a readiness
probe. Signal lock failure remains typed `Unavailable`.

This bounds scheduler visits, not the provider's own requested work or physical
source-index comparisons: the source map remains a B-tree. Complete operation cost,
all-platform acceptance and deployment topology remain separate obligations. The
existing production-scheduler Loom publication/recheck models remain in place.

`python3 rust/proofs/qualify.py ready-source-race` selects the existing production
Loom publication model. Two original removed-source callbacks publish during the
replacement's provider poll while its actual `Gate` producer races publication.
One callback has the replacement channel's stale generation. The retained parking
mutation must lose the original publication wake; exact restoration must complete
the same unchanged schedule. This checks concurrent parent registration on the
shipping scheduler and Signal through their Loom synchronization adapter, rather
than a parallel scheduler model. External provider wire formats, physical process
restarts and full deployment topology remain outside this component model.
