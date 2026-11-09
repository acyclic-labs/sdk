# Daytona Machines provider: decisions and unresolved transport contract

Status: blocked, 2026-11-09. The requested Cloud Swarm implementation is not
complete. This document records the prerequisite needed before an adapter can
honestly offer shell execution and workspace file round-trips. No provider code
or conformance changes accompany it.

## Decisions

- Selected-volume attachment must return `ProviderError::Unsupported`. Mapping
  the selected `VolumeRef` onto Daytona's native volumes is out of scope.
- A provider without attachment must use one shared copied-workspace path in
  the harness workspace layer, retaining the original selected volume,
  authority verification and filesystem publication through the git facade.
  Relaxing the selected-volume observation check alone does not implement
  that path or make shell and file tools share a workspace.
- Credentials remain behind the existing `CredentialResolver::bind` boundary:
  canonical `SecretRef`, `https://app.daytona.io`, bearer scheme, protected
  fetch. Neither redirect following nor sending that binding to an additional
  origin is authorized by the requested contract.
- An unsupported native mechanism must refuse explicitly. Whole-file
  download/re-upload, shell/base64 file access, combined stdout/stderr, readiness
  polling and fabricated operation receipts cannot substitute for Machines
  guarantees.

## Blocking prerequisite

The spec fixes the credential binding and Fetch egress to `app.daytona.io:443`.
Daytona's published v0.231 shell and file examples instead address
`https://proxy.app.daytona.io/toolbox/{sandboxId}/...`. The restored adapter at
`485a09e398b^` also obtains `toolboxProxyUrl` from the sandbox and sends its raw
key to that URL. Keeping that behavior would break the requested credential
boundary. Replacing it with protected fetch still leaves the additional origin
unauthorized.

The published file API exposes whole-file upload/download. Its documented
contract does not provide offset writes, short-write completion or native
errno, which the current `MachinesProvider::write_file` requires. The v2 alpha
file reference likewise documents multipart file replacement rather than the
native range-write protocol. A deterministic fake server must not invent such
routes and be presented as proof that Daytona implements them.

To unblock implementation, the platform needs to identify an admitted toolbox
transport (including its origin, protocol version and credential ownership)
that supplies the required execution and file semantics, or explicitly revise
the file contract and authorization scope. No same-origin toolbox route or
native range-write contract was supplied with this task. The pending question
asks for that missing transport information; it does not request a raw key.

Sources, checked 2026-11-09:

- [Daytona v0 process and code execution](https://www.daytona.io/docs/en/process-code-execution/)
- [Daytona v0 file operations](https://www.daytona.io/docs/en/file-system-operations/)
- [Daytona v2 alpha file API](https://www.daytona.com/docs/api/toolbox/files)

## Repository findings for the implementation

- `harness/src/wasm/agents/workspace.rs` constructs `grpc::Provider` after
  decoding the admitted `ProviderRef`; adapter selection belongs at this
  composition boundary and must be callable by native composition too.
- `harness-providers/src/http.rs` and `http/browser.rs` currently send POST
  only. Daytona REST needs HTTP methods preserved by the shared transport.
- Credential resolver and HTTP types currently belong to `harness-providers`,
  which optionally depends on the harness facade. Making that facade depend
  back on a Daytona adapter using those types creates a browser dependency
  cycle. Move the shared transport/credential boundary into a lower shared
  layer, preserving the model provider's exports and existing tests.
- `machines::conformance::verify` assumes checkpoint/fork, suspension,
  operation watches, history and usage are supported. General capability
  declarations must drive the checks and require typed `Unsupported` results
  for unavailable mechanisms, while retaining all existing supported checks.
- `machines::conformance::verify_attached` tests owner-enrolled attached devices
  and offline/reconnect behavior. It does not test `attach_volume` or copied
  workspaces. A separate shared workspace round-trip case is necessary.
- The current `MachinesWorkspace` requires the selected volume in the settled
  machine contract. No shared remote copy-in/commit-out implementation was
  found at this boundary; adding one requires actual guest file access.

## Validation

No Rust build or test was run on the PC or on bb2. The incomplete implementation
was removed rather than publishing a passing fake-only file protocol. The
resulting branch contains this decision record only; it is not ready for the
Cloud Swarm critical path. AI SDK, pi-ai and ComputeSDK remain deferred.
