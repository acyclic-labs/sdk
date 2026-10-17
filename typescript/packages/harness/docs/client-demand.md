# Demand and request composition

The host is trusted to enforce the reserved bound **before** reading/allocating,
authenticate correspondence, return transitively immutable values and account
deep/backing storage in its byte receipt. Cancellation is advisory for physical IO and does not reverse effects.

`HarnessClient` queues request files in a private volume and publishes them in
order. `ClientQuery` rebuilds its Rust views from a snapshot and follows from
that exact cursor; failed output retains its delivery for retry. Choose `Durable`
for persistent providers and `Accepted` for process-local memory sessions.
