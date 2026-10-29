# Objects operation admission

`GrpcObjects::with_context` and `HttpObjects::with_context` retain the original
Auth Context. They do not create a deadline, cancellation source or refresh
worker. Connection setup supplied by the caller runs under that same Context
before the authenticated channel is adopted. Lazy channel readiness and HTTP
connection setup are inside the request wait.

Safety follows three boundaries. Semantic preparation observes the stop before
hashing, request construction or callback invocation. A credential owner can be
reentrant: the shared native channel and HTTP client therefore observe the stop
after owner admission and immediately before transport dispatch. Finally, both
the shared Context and semantic admission observe the stop before accepting the
completed result. Cancellation produced by completing work wins over its result
in that same poll. Local stop statuses retain their typed cause and cannot be
mistaken for a remote unauthenticated rejection of a credential. Existing exact
lease and revision checks remain in force.

Progress has one producer per wait. Credential resolution waits on its owner;
transport I/O waits on the maintained channel or socket. The host's original
stop event races that complete wait, once. The host defines how cancellation
sender loss maps to the stop; the controls map loss to `Stopped::Cancelled`.
That event drops the in-flight future and its owner interest. There is no
readiness timer, background refresh task, retry or polling loop. Clones retain
the terminal stop, so a later call cannot restart a stopped operation. A local
stop does not prove remote rollback, and cancellation uses `OutcomeUnknown`.

Required work, relative to an unbound call:

| Work per operation | Before | Bound operation |
| --- | --- | --- |
| New relative gRPC timeout registrations | 1 | 0; reuse the host's Context |
| New relative HTTP request timeout registrations | 1 | 0; reuse the host's Context |
| Active Context wait registrations in the transport | 0 | 1, released with the call |
| Semantic Context observations | 0 | 2: before preparation and after conversion |
| Native dynamic credential boundary observations | 0 | 2: after resolution and after dispatch admission |
| HTTP boundary observations | 0 | 2: before send and before JSON decode |
| Final Context observation in `Context::run` | 0 | 1, including same-poll completion |
| Extra operation event in request extensions | 0 | 0; the guarded channel retains one Context clone |
| Added inline ownership fields | 0 | One optional Context per client and guarded channel, one optional typed stop per Objects error |
| Added tasks, network requests, durability barriers, payload allocations or payload copies | 0 | 0 |
| Wire deadline projection | Relative monotonic deadline | One wall-clock observation maps the original absolute deadline; resolution consumes the same remaining budget |

Context cloning retains the existing shared event; it does not allocate another
timer or event producer. Context observations use that primitive's existing
waiter bookkeeping. Its slab may allocate when simultaneous observers exceed
its previous capacity; insertion is amortized constant work, not a claim of
worst-case allocation-free or lock-free progress. Stopping shared waits wakes
each current observer once. A local stop crossing tonic allocates one `Arc` to
retain the typed source, as required by `tonic::Status`; direct Objects stop
admission stores its reason inline. Unbound native calls add one monotonic
observation before resolution so an expired call cannot invoke a callback.
Admission adds fixed work per boundary, independent
of credential history, account count and completed requests. Payload hashing,
encoding, decoding and I/O remain proportional to the supplied bytes; revision
requirements retain their existing authority-dependent cost. No end-to-end
constant-time or latency improvement is inferred from these static counts.

The controls use the production Context, credential owner, guarded native
channel, real HTTP/2 service and real loopback HTTP gateway. They cover expiry,
cancellation and producer loss during refresh and body/RPC waits; clone reuse;
reentrant owner cancellation; an explicit budget above the old 30-second
default; and 1/100/1,000 concurrent stopped calls with zero callbacks. Healthy
releases and fresh operations on the same channel preserve progress.

Counterfactuals remove final stop observation, pre-dispatch observation, original
budget binding or restore the HTTP relative timeout. Each must fail its specific
admission, progress or cost assertion. A fixed token already has zero remote mint
calls: zero callbacks after a stop proves no new credential work, not removal of
a token RPC from an otherwise successful request.
