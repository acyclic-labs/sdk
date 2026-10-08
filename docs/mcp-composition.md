# MCP composition

Slice F extends the ordinary versioned `ToolRegistry`; it does not add a tool
scheduler, registry, process owner, or effect journal. MCP is explicitly selected
and construction performs no I/O. The current source is **unqualified work in
progress** until the evidence table below is completed on its final revision.

`McpCatalog::install` validates the entire replacement before publishing it. Its
consumer-selected server namespace is one component (no dots), independent of
the remote implementation name. Successful replacement changes future model
selection, retains removed/old revisions, and rejects a stale previous catalog.
Each executor retains its exact remote name, schema and transport binding.
The host must explicitly choose `schema_exposure` (`eager` or distinct selected
remote names) and `discovery` (`disabled` or bounded local `search`). Selection
changes model-visible schemas, not the complete catalog or retained executors.
Discovery can find a hidden schema but supplies no call grant. Both policies are
pinned by the host configuration revision and must be explicitly reloaded.
The existing registry retains the complete catalog digest, including policies,
so stale replacement fails even when visibility and catalog contents are empty.
Rust `model_definitions` and generated `mcpModelDefinitions` return the same
canonical exposure projection; no JavaScript selection engine is introduced.
Remote output schemas validate `structuredContent`; the canonical Harness result
is the MCP content/result envelope. Consumers supply the ordinary result
projection. D owns generic variant and projection changes.

`HttpMcpTransport` binds the `rmcp` JSON-RPC contracts and `sse-stream` parser
to Harness operation identity, session headers and finite byte allowances.
`McpHttpProvider` supplies platform I/O. Its contract
requires a finite whole-exchange deadline, cancellation on stream drop, no
automatic POST retry and no cross-authority redirect. The optional `mcp-http`
feature supplies `NativeMcpHttpProvider` using the workspace's existing reqwest
dependency. Native construction disables redirects/retries and performs no I/O.
The generated WASM bridge uses the same trait and Rust decoder. The TypeScript
browser provider supplies Fetch, byte reads and cancellation only; it shares the
ordinary task AbortSignal and enforces the finite whole-exchange deadline.

Initialization and discovery are explicit admitted host operations. A host stages
`initialization_request`, dispatches it through its admitted provider route,
retains the observation, and calls `accept_initialization`. This validates the
negotiated protocol/session and returns the initialized notification for separate
admission. A tool call cannot silently initialize/reinitialize. `list_tools`
returns one typed bounded page. The host collects a complete catalog under its
declared total bounds before publishing replacement; a partial page must never
withdraw tools from the installed catalog. Search uses that pinned catalog and
grants no permission to invoke a result.

The existing executor authorizes `mcp:call:<namespace>` before replay, dispatch
and reconciliation. HTTP request IDs do not promise server idempotency. Native
HTTP has no remote receipt API: reconcile returns `None` without network I/O.
Expired sessions and interrupted calls remain indeterminate; future setup can
admit a new session but cannot resubmit the old uncertain call.

G owns credential/OAuth flows, resources, prompts, sampling, elicitation and
server request routing. This client advertises no callback capabilities and
rejects server requests requiring such a router. Ordinary notifications do not
cause an implicit reload. A typed callback seam and retained notification policy
still need coordination with G before F qualification.

PR5's landed native request uses null stdin and bounded one-shot output capture.
An approved MCP stdio exchange needs a small extension of that existing admitted
process path; it must reuse its launch/observation receipts, exact approval,
lease checks and process-tree drain. This extension is not implemented yet.
No raw process launch or replacement process journal is an acceptable fallback.
`McpStdioExchange` wraps `rmcp`'s bounded newline codec for that extension.
It validates initialization before emitting the initialized notification and
one request, pins distinct IDs, and fails permanently after malformed output.
Its parser state is transient; it does not substitute for native durable receipts.
The ingress adapter validates bounded complete frames as `RawValue` before the
library codec sees them, preventing its malformed-input debug event from logging
private stdout. The codec decodes raw JSON; Harness then hydrates it using its
existing unbounded-depth JSON contract. A retained scan offset visits incomplete
prefix bytes once. Development controls exercise 192-level JSON, split BOM/CRLF,
and malformed private input, with an unguarded library log as a negative control.
Native process integration and final-source qualification remain open.

## Simplification audit

| Retained mechanism | Contract requiring it |
| --- | --- |
| Registry selected value is `Option<revision>` | Distinguishes ambiguous replacement from explicit withdrawal while retaining admitted versions |
| Catalog clone before publication | A failed batch cannot partially alter the next model-visible catalog |
| Catalog digest in the existing registry | Empty/hidden visibility must still reject stale complete-catalog and policy replacement |
| Separate remote schema and canonical result envelope | MCP structured output differs from ordered multimodal content |
| Library SSE parser with pre-parser byte bound | Established framing implementation handles split UTF-8/CRLF; no unbounded hydration before admission bounds |
| Shared `rmcp` JSON-RPC boundary | HTTP and stdio enforce the same response identity and notification contracts |
| Bounded stdio exchange codec | Negotiation precedes the one admitted request; malformed output cannot restart it |
| Raw JSON ingress validation and scan offset | Keep private malformed stdout out of library logs, preserve the admitted depth policy, and avoid rescanning incomplete prefixes |
| Reconciliation distinct from HTTP POST | Uncertainty cannot cause an unapproved remote replay |
| Optional native dependency | Portable/browser construction must not require native network/process providers |

No replaced MCP implementation exists in Harness. The Codex adapter's MCP
configuration is a separate external-product integration and is not superseded
by this reusable client.
The initial development parsers and content-shape checker in this branch were
removed in favor of `rmcp` and `sse-stream`. Their autonomous service, HTTP worker,
session recovery and child-process features are not selected; ordinary Harness
admission, retained receipts and the PR5 process owner remain authoritative.

## Qualification ledger

| Invariant | Production path | Assumptions | Required evidence | Status |
| --- | --- | --- | --- | --- |
| Atomic reload and retained revision | `McpCatalog::install`, `ToolRegistry` | Host retains installed catalog/configuration identity | Exhaustive replacement subsets; stale/malformed/duplicate controls; actual in-flight call | Pending |
| No authority by discovery | Catalog search, executor authorization | Ordinary runtime owns signed scope/task admission | Missing/wrong grant; admitted executor integration | Pending |
| Explicit schema/discovery policy | Catalog installation and bounded search | Host selects eager or named schema exposure and retained discovery policy | Both exposure modes, malformed selection, reload/in-flight policy pinning | Implementation and tests added; qualification pending |
| No uncertain HTTP replay | Tool journal + provider reconcile | Server has no receipt API unless explicitly supplied | Faults before/after remote apply and local observation; actual durable restart | Pending |
| Exact HTTP/session/protocol | Rust request/response and SSE decoder | Provider normalizes headers and enforces deadlines | Real local JSON/SSE/session fixture, all stream cuts and malformed controls | Pending |
| Approved stdio | Existing PR5 process/effect owner | Actual approved provider supports bounded stdin exchange | Real subprocess, cancellation/drain/host crash/reopen | Not implemented |
| Portable contracts | Same Rust provider platform and decoder | Browser host implements network I/O only | Generated TS, WASM, Chromium reload/workers; installed artifacts | Pending |
| Platform correspondence | Owned final source | Shared-host lease/grants respected | Windows, WSL/Linux, macOS `ssh ivar`, required full CI | Pending |

These are engineering acceptance gates, not an unrestricted proof. Bounded
production checks must disclose their enumerated states and negative controls.
No open PR, compile check, skip, or partial platform receipt completes the goal.

### Disk-backed HTTP fault matrix

`tests/mcp_durable.rs` drives the production `DurableToolRunner`, real native
HTTP provider, `FilesystemExecutionJournal`, LocalStream and local Filesystem.
It drops all client/provider/journal/runtime handles between phases and rebuilds
them from disk with the same operation/catalog/session identity. An already
admitted task scope is supplied by a fixture TaskStateProvider; this test does
not qualify the complete scheduler/task host or an OS process-kill recovery.

The seven enumerated cases are: normal success; failure immediately before the
dispatch claim; lost claim acknowledgement after commit; remote apply with lost
HTTP response; result staging failure after remote apply; failure before terminal
publication; lost terminal acknowledgement after commit. A server counts actual
applies. Only the pre-claim case may make its first dispatch on reopen; a committed
claim is never reposted. A committed terminal result is replayed. Both the initial
run and the run after the library replacement passed all seven cases. The latter
command also passed all 250 library tests with `filesystem-local,mcp-http`.
This is Windows development evidence; the final platform/installed/
crash gates remain required.

### Initial development checkpoint

Base: `6aaed7e3a49a609c8a62790356ce94588d2e44f5`, branch
`codex/harness-mcp-transports`, 2026-10-08. This is development
evidence, not final-source qualification or a landed capability.

- Windows `cargo test -p acyclic-harness --no-default-features --features mcp-http --lib --locked -j 2`:
  231 passed, zero failed/ignored. Twelve MCP tests cover schema/namespace/authority,
  initialization, JSON/SSE, expired session, 64 replacement transitions and real
  loopback JSON/SSE/lost-response/redirect controls, bounded stdio stream cuts,
  and a paused remote executor call across catalog/transport replacement. The subset enumeration is
  a bounded check of production transitions, not an unrestricted proof.
- Strict Windows library/test Clippy with the same feature selection and
  `-D warnings`: pass. No lint suppression was added.
- `node scripts/build-wasm.mjs harness`: pass using two jobs; generated MCP
  declarations contain one interface each and reuse Rust-derived JSON types.
- Node WASM catalog/browser bridge tests: five passed,
  zero failures/skips. A real Map-versus-JSON schema mismatch was found and fixed
  in the Rust facade; the accepted result includes number/object preservation.
- Isolated strict TypeScript check of `src/mcp-http.ts` with global `tsc` and
  DOM libraries: pass after converting generated header Map entries to the
  DOM constructor's tuple-array type. This does not substitute for the pinned
  workspace toolchain or complete package typecheck/installed-artifact gates.

Actual Chromium execution, durable HTTP fault/restart integration, approved stdio,
remaining platform/installed gates and final simplification remain open. Search
explicitly bounds the entire scanned catalog; its current cost is linear in the
selected catalog size. It does not claim indexed discovery or unbounded scale.
The WASM facade validates/searches catalogs and executes HTTP through the shared
Rust transport. Node WHATWG fixtures exercise the browser provider, including
task/deadline abort and uncertain responses; those are not Chromium receipts.

### Explicit policy development checkpoint

Eager and selected schema exposure and disabled/search discovery now have required
Rust-derived fields and the same canonical native/WASM projection. In addition
to the 64 catalog membership transitions, the production registry test enumerates
64 selected-visibility transitions over a complete three-tool catalog, checks
every old/new executor revision remains retained, rejects stale/absent previous
catalogs even for empty visibility, and includes missing/duplicate-name controls.
The existing empty-catalog stale control now runs for every catalog subset.

Windows library tests with `filesystem-local,mcp-http`: 251 passed, zero failed/
ignored. The disk-reopen fault matrix also passed. Strict Clippy over library and
all test targets passed. WASM rebuilt successfully and six Node catalog/browser
bridge tests passed, including eager/selected/empty exposure and independent
discovery controls. The isolated browser-provider TypeScript check passed.
These checks are development evidence; final-source cross-platform, installed,
Chromium and native stdio gates and the registry seam owner review remain open.

### Stdio ingress development checkpoint

The library codec now decodes raw JSON after bounded syntax validation; the shared
Harness decoder owns hydration. Four stdio tests pass, including 192-level JSON
at several stream cuts and split BOM/CRLF. The private-output log test first
proves the unguarded dependency emits its marker, then verifies the guarded
exchange rejects the same frame without a log or later processing.

Windows library tests with `filesystem-local,mcp-http`: 253 passed, zero failed/
ignored, and the seven-case disk-reopen matrix passed. Strict Clippy over library
and all test targets passed without suppressions. WASM rebuilt successfully and
all six Node catalog/browser bridge tests passed, with no generated declaration
changes. These remain development checks, not native-process, actual Chromium
or final-source platform qualification.
