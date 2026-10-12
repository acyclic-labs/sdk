# Demand and request composition

The host is trusted to enforce the reserved bound **before** reading/allocating,
authenticate correspondence, return transitively immutable values and account
deep/backing storage in its byte receipt. Cancellation is advisory for physical IO and does not reverse effects.

Start one `ProjectionStore` per durable authority with a `ProjectionRecovery`
adapter. Its identity includes the security namespace, reducer/schema version and
domain adapter. Use the same `IndexedDbClientStore` for outbox and cursors; custom
stores must implement its atomic `commit` and `loadRecovery` contracts.
