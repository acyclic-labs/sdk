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
crate validates customer-authored requests through its typed semantic domain
(`ActorId`, `CodeSha256`, `PositiveU64`, finite states, nested records, and
presence-checked requests) before a request reaches either transport. Native
N-API and browser WASM adapters carry only bounded protobuf bytes; they decode
into that same domain and encode responses only after fallible semantic
conversion. The package descriptor is generated from the maintained Rust
contract declarations and is used to generate TypeScript bindings.

`grpc::connect(endpoint, token)` exposes every generated Actors service RPC.
It trusts the platform's roots (on Linux, those `SSL_CERT_FILE` and
`SSL_CERT_DIR` select). Use `grpc::connect_with_ca_certificate` for a
caller-supplied private CA; that CA is then the whole trust.
`http::Client::new(endpoint, token, maximum_response_bytes)` exposes the same
nine operations using canonical Protobuf JSON. Invocation carries request and
response headers. HTTP mutations are not automatically retried.

## Deployment secret selections

Actor and Worker deployments carry an ordered `secrets` list of `{name,
reference}` declarations. Namespace configuration updates carry the same list.
The list is a full replacement: an empty list selects nothing. Bindings and
secrets share one environment-name namespace; duplicates and collisions are
rejected by the canonical Actors admission function.

Manifest lowering resolves only names explicitly listed in `entry.secrets`
against `environment.secrets`. Declaring a secret in an environment grants no
entry access to it. Both runtime kinds use this same lowering function.

References use the bounded `secrets:/scope/name` grammar. They are addresses,
never plaintext, versions, bearer tokens or unwrap credentials. Resolution and
authorization belong to the deployment host; SDK lowering never reads a value.

### Mixed rollout

`SecretSelection` uses new message fields 1 (`name`) and 2 (`reference`).
DeploymentRequest appends field 12; UpdateNamespaceRequest appends field 7.
Existing field numbers, wire kinds and meanings are unchanged. Writers never
encode selections as bindings, reuse predecessor fields or send credential
values. Empty lists emit no new fields.

The retained predecessor descriptor was captured from SDK main `b7333953cb`.
Executable tests decode new messages with that actual reader and verify that
every known predecessor field is unchanged. They also decode predecessor
messages with the new reader and require empty selections.

Unknown-field compatibility does not make an old deployment host resolve
secrets: it ignores the new fields. Deploy hosts that support selections before
enabling nonempty-selection writes. A host must authorize and resolve every
selected reference before accepting a deployment; it must reject an unsupported
selection rather than acknowledge a deployment with missing environment values.
There is no fallback to environment inheritance or plaintext bindings.
