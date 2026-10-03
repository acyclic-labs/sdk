# Managed Agent Runtime

Managed Agent Runtime is the planned hosted composition of the open-source
Harness. This guide records the intended customer contract from Rust-owned
semantics. It does not advertise a public managed endpoint, capacity, price, or
service guarantee.

## The same harness, operated for you

Managed Agent Runtime adds managed capacity, deployment, recovery operations,
and organization controls around the open-source Harness. It does not introduce
a separate agent loop, reserve recursive tasks for paying users, or require a
Graphcoder as the interface.

The complete Harness remains useful with local or customer-selected providers.
Managed hosting is one way to operate it; using an individual Acyclic service
does not require managed hosting.

## Prepare a managed run

A qualified host must:

1. Register the versioned task or harness build and its supported resumable
   components.
2. Bind models, tools, source and artifact access, and execution requirements;
   scoped secret references come through host configuration.
3. Select effective concurrency, resource, deadline, retention, and grant
   policies. Unsupported capabilities reject explicitly.
4. Connect through the normal Harness API, admit tasks, observe their handles,
   and route interactions to the application or an authorized policy service.

The following is a proposed shape only; no managed endpoint is advertised:

```rust
// Proposed API; no public managed endpoint is advertised.
let harness = Harness::connect(managed_host).await?;
let review = harness.task::<ReviewInput, Findings>("review")?;
let task = harness.spawn(&review, input).await?;
let outcome = task.result().await?;
```

## Placement and data boundaries

The agent loop, tools, memory, files, and models may live in different places. A
private-network tool can remain in the customer environment while independent
tasks execute on hosted Machines. Placement requirements specify accessible
resources and trust domains; a connection does not authorize data movement.

Managed tenant isolation, grant enforcement, and provider receipts cannot be
replaced by customer plugins. Fresh child identities remain within delegated
authority. Secrets, open sockets, and local process handles must not be
serialized into task state.

## Continuation and operator responsibilities

Supported resumable components continue from recorded boundaries after host loss.
Live-only Rust work requires a compatible process lifecycle or an explicit
restart; hosting alone does not make it durable. Cancellation is a request,
observation can reconnect, and ambiguous effects require reconciliation.

Retention and source or export access are configured separately from active
execution. The host reports expired references, incompatible code versions, and
unsupported recovery rather than silently restarting from scratch. A release
change pins existing operations or uses a supported recorded migration.

## Qualification before availability

This page defines the intended customer experience, not an available service.
Publication requires a usable Harness release, documented deployment and support
profiles, recovery and isolation evidence, and measured workload performance.
Capacity limits, prices, and service guarantees remain unpublished until
qualified. Usage comes from selected providers' receipts and any published
managed-service charges; the Harness does not invent a fifth Inference meter or charge a child again at each ancestor.