# Daytona Machines provider status

The origin and range-write decisions were resolved on 2026-11-09. The adapter
now uses the shared host credential/HTTP boundary in harness-core; model
providers re-export that one boundary. No raw credential enters provider config
or guest file contents. Every toolbox URL comes from the sandbox response and
must bind its exact origin through CredentialResolver::bind. Tests use exactly
https://app.daytona.io and https://proxy.app.daytona.io.

Daytona declares native range reads/writes and attach_volume Unsupported. The
shared Machines file seam selects by capability and composes whole-file
download/upload under exclusive file ownership. Both the client and harness host
use that seam. The shared file suite covers chunking, binary bytes, overwrites,
sparse extension, zero-byte writes and truncation for native and whole-file
providers. It claims no native errno or positional-write atomicity for uploads.

Create keeps an acyclic-{key} name and versioned request/contract labels. A lost
reply never authorizes redispatch. Explicit recover reads the original key label.
Mutation settlement uses its response plus at most one inspection. There are
no readiness loops or fabricated operation/watch/cleanup receipts.

Machines without attach_volume use the shared copied workspace
(harness-core workspace/copied.rs). At bind it copies the selected volume in
through the workspace facade. At settlement it publishes the authored changes
under the original turn and authority. The path is selected by attachment
capability, so every non-attached provider uses it, not only Daytona.

Shell execution uses request/response exec when a provider declares binary duplex
Exec Unsupported. Daytona v0 exposes text session input/logs, not the exact-byte
stdin/EOF/channel/terminal-cleanup contract, so the adapter declares binary
duplex Exec Unsupported by design. Other unavailable lifecycle mechanisms are
also explicitly Unsupported.

The deterministic HTTP fixture exercises the production native HTTP transport,
records host-injected authorization on every exchange, refuses unbound origins
before toolbox I/O, and checks lost-create recovery without redispatch. Native
file coverage uses the real local Machines provider and the same file suite.

Sources checked 2026-11-09:

- [Daytona v0 file operations](https://www.daytona.io/docs/en/file-system-operations/)
- [Daytona v0 process and code execution](https://www.daytona.io/docs/en/process-code-execution/)
