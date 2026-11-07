# Public operations and protocol descriptors

An RPC inventory is not the complete SDK operation inventory. Enumerate every
service in every product descriptor, HTTP-only routes in
`acyclic.validation.v1.http_routes`, and the semantic operations carried by
shared RPCs. Local signing, verification, projections and host callbacks have no
independent remote endpoint.

The descriptor roots are declared in `acyclic_proto_build::FAMILIES`
(`compatibility/src/lib.rs`). This includes `filesystem-wire`, Auth and the
Harness contract, as well as Actors, Inference, Machines, Objects and Streams.
Selecting only descriptors that currently contain an exposed service omits
HTTP-only Auth and the filesystem's separate contract. A declaration establishes
the public contract; it does not establish that a deployment implements it or
that any scenario has passed.

## Machines

`MachinesService.Checkpoint`, `Fork` (from a checkpoint) and `ForkMachine`
(directly from a machine) are separate declared operations in
`rust/crates/machines/proto/machines/v1/machines.proto`. Their SDK owner is
`MachinesProvider::{checkpoint,fork,fork_machine}` in
`rust/crates/machines/src/provider/mod.rs`. Unsupported backend capabilities
must retain the declared refusal; an unsupported operation is still part of
the public contract.

`MachinesProvider::recover_operation` uses the existing `Recover` RPC and
returns its operation identity without waiting for settlement. `assurance` is
local provider metadata. `Machines::attach` inspects the existing machine through
`InspectMachine`; `Machine::identity` computes its local resource identity.
These add no remote endpoint. The implementations
are in `rust/crates/machines/src/grpc/provider.rs` and
`rust/crates/machines/src/provider/client.rs`.

## Auth

`secrets::Client::{put,list,delete}` in `rust/crates/auth/src/secrets.rs` are
remote HTTP operations. Their POST routes, authorization actions and resource
templates are declared in the Auth descriptor's `http_routes` file options;
their request and reply messages are in that same descriptor. The HTTP client
and contract renderer share the route constants. They are not additional gRPC
services. There is no protected-value get, reveal or export endpoint.

The following public operations have no independent network endpoint:

| Source | Symbols | Why no additional remote operation exists |
| --- | --- | --- |
| `rust/crates/auth/src/verify/signer.rs` | `CertifiedSigner::mint` | Signs locally with the runtime custodian's certified leaf; no signing key is sent to a service. |
| `rust/crates/auth/src/verify.rs` | `verify`; `VerifiedCapability::{check,authorize,attenuate,forward,seal,encode_sealed,encode}` | Local authentication, authorization, delegation and encoding of the existing capability messages. |
| `rust/crates/auth/src/verify/certificates.rs` | `CertificateVerifier::{verify,verify_certificates}` | Local certificate authentication against supplied current trust. |
| `rust/crates/auth/src/authority.rs` | `decode_block`, `decode_caveat`, `decode_capability`, `attenuate`, `permits` | Local decoding and policy evaluation; no credential issuance or dispatch. |
| `rust/crates/auth/src/trust/issuer.rs` | `Issuer::{environment_keyring,genesis,keyring_for,keyring,birth,certify,allowance}`; `AccountRoot::{genesis,possession_proof,resume,certify,certify_request,revoke,revoke_keys,raise_minimum_epoch}`; `KeyringFrontier::{resume,apply}`; `verify_possession` | Custodian signing, proof verification and replay produce existing certificate/keyring/revocation messages. Publication belongs to the original owner's Streams, not an extra Auth RPC. |
| `rust/crates/auth/src/trust/follower.rs` | `EnvironmentFrontier::resume`; `Follower::{apply,apply_environment,apply_account,apply_queued,release,fence,connect,close,resume,serve,refuse,snapshot}` | Local trust replay, admission fencing and observation of the owner's existing Streams. |
| `rust/crates/auth/src/revocation.rs` | `RevocationState::{from_checkpoint,apply,trust}` | Local authenticated revocation index and replay. |
| `rust/crates/auth/src/trust/issuance.rs` | `Client::{issue,recover}`; `Provider::{issue,recover}` | Transport-independent issuer callbacks. Existing `IssuanceRequest`/`IssuanceReply` messages describe the exchange; the adapter supplies the original issuer's authenticated transport and transaction. The SDK defines no new URL or gRPC service here. |
| `rust/crates/auth/src/credential.rs` | `CredentialProvider::{resolve,watch}`; `ResolvedCredential::{check,reject}` | Host-owned credential custody callbacks and events, with no fixed remote service. |
| `rust/crates/auth/src/guest.rs` | `GuestAdmission::{admit,project,changed}` | Invocation-local opaque handles; never a remote credential or secret retrieval API. |
| `rust/crates/auth/src/secrets.rs` | `Resolver::resolve`; `decode_request`; `put_target`, `list_target`, `delete_target` | Trusted host secret resolution, boundary decoding and authorization targets; no public value-reveal endpoint. |

Address/resource parsing, cryptographic transcript helpers, operation contexts,
constructors and getters are local library operations too. TypeScript and Wasm
adapters project these same owners; they do not introduce another service.

## Shared carriers and composed agent operations

`AgentControls::{discover,discover_at,select,execute}` in
`rust/crates/harness-client/src/control.rs` use canonical Actors invocation on
the caller's existing authenticated Streams Session. The command's public JSON
contract is `AgentCommand` in `rust/crates/harness-client/src/command.rs`:
`Bind`, `Root`, `Prompt`, `Cancel`, `Compact`, `Rewind`, `Restore`, `ClearAll`,
`Spawn`, `Fork`, `Children`, `ProjectHeads`, `History`, `Approvals`, `Decide`,
`Reply`, `Settlement`, `Diff`, `Usage`, `Events`, `Read`, `Stage`, `Staged`.
These are semantic operations within `InvokeActorRequest.body`, not separately
declared protobuf methods. Record each command's coverage even when several use
the same carrier RPC. Staging is the existing bounded transport for one command,
not a second execution mechanism.

`AgentControls::{observe,observe_all}` and `AgentClient::{set_view,set_viewport,
open_resource,release_resource,next}` in `control.rs` and `agents.rs` compose
existing Session interests and local bounded projections. Likewise Session
credential replacement, interest removal, flow credit, acknowledgements and
streamed actor input are distinct protocol branches within
`rust/crates/stream/proto/stream/v1/session.proto`, not new RPC methods.
Machines Exec input/control frames and filesystem transaction operations also
need branch coverage within their declared carriers. An RPC-level happy path
alone does not cover those operations.

Local backend/provider traits, filesystem mounts and guest host imports are
in-process or OS interfaces. Their shared conformance obligations still apply;
they do not imply additional network endpoints or another wire protocol.
