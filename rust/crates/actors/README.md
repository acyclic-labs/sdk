# acyclic-actors

Rust-owned Actors v1 public contract. An Actor has an identity separate from
Streams, an immutable code version, explicit bindings, and independently
recoverable subscriptions. Adding a subscription selects its start once;
delivery and safe cursor advancement are service-owned. Pausing follows a
bounded handler failure, and explicit resumption can replay external effects.
Code and bindings are updated by a full configuration CAS. Checkpoint schema
compatibility or migration is checked before activation; a failed update keeps
the previous version active, and paused subscriptions remain paused.

The service owns execution, fencing, checkpoint storage, and hibernation. This
crate validates customer-authored requests and packages the versioned wire
descriptor used to generate TypeScript bindings. Native builds expose direct
gRPC and HTTP adapters; the shared client selects gRPC or gRPC-Web, while
browser consumers may also use the TypeScript Actors transport over the same
canonical contract.

`client::connect(endpoint, token)` selects the canonical authenticated
transport for the target: native builds use gRPC and browser builds use
gRPC-Web. Use `client::connect_with_ca_certificate` when a native caller pins
a private CA. The semantic client accepts the typed domain requests and
returns typed responses; invocation carries request and response headers.

## Canonical transport call

This example uses the target-aware client and the semantic domain request. It
is compile-checked by rustdoc and does not contact the example endpoint while
documentation is built.

```rust,no_run
# async fn inspect() -> Result<(), Box<dyn std::error::Error>> {
let client = acyclic_actors::client::connect("https://actors.example", "account-token").await?;
let request = acyclic_actors::domain::InspectActorRequest::new(
    acyclic_actors::domain::ActorId::new("actor-a".to_owned())?,
);
let response = client
    .inspect_actor(&request)
    .await?;
if let Some(actor) = response.actor() {
    println!("{}", actor.actor_id().as_str());
}
# let _ = response;
# Ok(())
# }
```

## Runnable examples

The repository keeps the transport and descriptor examples as executable Rust
sources:

- [`actors-http-routes`](examples/actors-http-routes.rs) emits the route table
  consumed by the TypeScript binding generator.
- [`conformance-certificate`](examples/conformance-certificate.rs) creates the
  ephemeral certificate used by local transport conformance.
- [`transport-conformance`](examples/transport-conformance.rs) exercises all
  eight Actors operations through both native transports against the local
  test service.
