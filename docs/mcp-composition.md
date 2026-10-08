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
process path; it reuses its launch/observation receipts, exact approval,
lease checks and process-tree drain. `NativeProcessRequest.mcp_stdio` now pins
the exact optional method, initialization/operation identities, parameters and
byte allowance in that existing approval and effect request. Absent descriptors
retain the closed-stdin wire contract and canonical approval bytes.
No raw process launch or replacement process journal is an acceptable fallback.
The existing `ProcessTree` capture loop now has a bounded input branch with
nonblocking partial writes, ordered callback writes and explicit protocol
completion. It keeps the same intent, output, deadline and cleanup owner;
completion closes stdin and terminates containment before bounded output drain.
No writer thread or separate process loop was introduced. The existing admitted
provider validates the descriptor and uses this input branch. Each approved
process effect initializes one MCP session and performs one discovery page or
tool call; this does not claim a persistent server supervisor. A stopped capture
remains uncertain. Complete native receipts use the same codec/typed response
boundary without performing its writes, including after provider recovery.
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
End-to-end model-tool/native binding, native MCP fault/restart controls and
final-source qualification remain open.

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
| Bounded input branch in the existing process capture loop | Backpressure must not prevent intent/deadline checks or owned cleanup; the ordered queue is bounded by the complete admitted input allowance |
| Optional exact MCP descriptor in the existing native request | Protocol method, identities, parameters and allowance require the same approval and durable effect identity as the executable invocation |
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
| Approved stdio | Existing PR5 process/effect owner | Actual approved provider supports bounded stdin exchange | Real subprocess, cancellation/drain/host crash/reopen | Development consumer, receipt faults, six disk-reopen cases and actual host-death control pass; model-tool binding and final platform gates open |
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

### Native process input development checkpoint

`ProcessTree` nonblocking stdin and its shared capture loop pass real Windows
controls for a stalled reader, a 4 MiB non-UTF-8 byte pattern with partial writes
and explicit EOF, a two-step handshake followed by a stalled server, input
overflow, parser failure and timeout with bounded output prefixes and confirmed
cleanup. A counterexample found zero progress on oversized Windows writes despite
capacity for a smaller prefix; bounded logarithmic prefix probing fixes that
without an arbitrary input-size ceiling. The queue counts already-written bytes
against the complete input allowance, so callback writes cannot reset the budget.

Native runtime library tests: 61 passed, zero failed, one existing ignored local
backend comparison. Strict native runtime library/test Clippy passed without new
suppressions. Harness library tests with `filesystem-local,native-execution,mcp-http`:
272 passed, zero failed/ignored. These tests exercise process primitives and the
existing approved consumer. Strict Harness library/test Clippy with that feature
selection passed. They do not yet prove admitted MCP process effects,
crash/reopen receipts or final-source OS gates.

### Exact stdio admission development checkpoint

The optional native request descriptor and its method union are Rust-derived.
The existing provider checks it before admission/binding and routes its bytes
through the shared owned capture loop. The absent field preserves a legacy
request's canonical approval digest. Approval controls first bind a valid
provider, then reject changed initialization/operation IDs, method, parameters
and byte allowance without a launch. Descriptor controls reject malformed
parameters, callback methods, continuation/task requests, aliased IDs and invalid
allowances. An initial control exposed an ignored snake-case continuation field;
both wire spellings are now rejected explicitly.

The public `approved-native-process` consumer also runs as a standalone Cargo
example test, without test-harness stdout contaminating the MCP stream. Both its
closed-stdin and approved MCP paths pass. The latter verifies initialization
order, exact Unicode/full-width structured output, real child-side publication
into the SDK volume, and provider drop/recovery with one observed tool application
and no new child-side write. Its journals/storage adapters remain memory-backed;
this is provider recovery evidence, not host death or disk-reopen qualification.
Stopped native captures remain indeterminate at the MCP response boundary even
if their prefix contains a complete response. Decoding a stored complete receipt
performs no process writes.

Current Windows source checks: 274 Harness library tests passed, zero failed/
ignored, the existing seven-case HTTP disk-reopen matrix passed, both standalone
consumer paths passed, and strict library/test/example Clippy passed without new
suppressions. WASM rebuilt and seven Node catalog/browser/descriptor controls
passed, zero failed/skipped. Generated declarations and consumers use the same
Rust request/method types and inert validator. Final platform, actual Chromium,
installed artifacts, MCP-native fault/restart and model-tool binding gates and
owner seam review remain open; nothing here establishes a qualified main landing.

### Native MCP receipt fault development checkpoint

The standalone approved consumer now injects four native receipt faults through
a test-only wrapper around the ordinary memory stream backend: rejection before
the launch commit, a committed launch with lost acknowledgement, rejection before
the observed-result commit, and a committed observation with lost acknowledgement.
The wrapper also hides the immediate idempotency inspection after a lost reply,
so recovery must use retained receipts rather than an immediately recovered reply.
Each case asserts that its selected fault was consumed.

An independent peer-side call log verifies that neither launch fault executes a
tool and that both observation faults follow exactly one applied call. Provider
drop/recovery without a native view recovers a committed observation and its exact
Unicode/full-width result; the other three cases remain indeterminate. A repeat
effect invocation returns the same status and performs no new physical write or
tool call. SDK publication is checked separately from physical application, and
missing output must be a typed not-found result rather than an arbitrary error.

A fifth control applies the tool and writes its physical output, then withholds
the response. The approved one-second capture deadline records a timeout with
confirmed containment cleanup. The native capture receipt remains readable, but
the MCP result stays indeterminate before and after provider recovery. The
uncompleted capture publishes no SDK output and repeat admission does not replay
the call. These controls share the existing process/effect owner; the test wrapper
does not implement an alternative journal or process engine.

The example's request construction, publication and result checks are shared
between positive and fault cases. Strict Clippy initially rejected the expanded
composition's complexity; the checks were factored without adding suppressions.
The positive native/MCP paths and five fault controls pass on Windows, alongside
274 Harness library tests and the existing seven-case HTTP disk-reopen matrix.
Strict library/test/example Clippy passes. These are development receipts with
memory-backed journals: MCP disk reopen and host death, model-tool binding,
final-source platform/formal/installed-artifact gates, actual Chromium, owner
review and actual-main landing remain open.

### Native MCP disk restart development checkpoint

The public consumer separates initial execution from receipt recovery and accepts
ordinary filesystem/stream backends. Its memory controls and disk controls use the
same preparation, task registration, approved process provider, authority adapters,
stored-result validation, SDK-publication checks and physical call-log checks.
Recovery reconstructs the existing task runtime with the retained task ID and
lease fence and calls `NativeProcessProvider::recover`; it receives no native
view and creates no alternative process, task registry or receipt engine.

Six Windows disk cases passed with real `Fs::local` and `LocalStream` stores:
completed response, launch rejection, lost launch acknowledgement, observation
rejection, lost observation acknowledgement and applied call with no response.
Every injected commit fault must be consumed. The initial function returns only
inert restart references and a temporary-directory owner. Before opening fresh
stores, weak references assert that the old stream and filesystem hosts have
dropped, covering the old runtime, content/result/approval adapters and process
provider. No local backend handle is retained by the restart caller.

After reopening, complete observations retain the exact Unicode/full-width
result; the uncommitted launch/observation cases stay indeterminate. The withheld
response retains its confirmed-cleanup timeout capture while the MCP outcome
stays indeterminate. SDK output is read from the reopened filesystem, independently
of the native peer's call log. The physical output is removed before recovery;
repeat effect invocation recreates neither that file nor a second call-log entry.

The final factored consumer passes its two positive paths, five memory fault
controls and six disk cases. Strict library/test/example Clippy passes without a
new suppression. The public example also passes a `native-execution`-only Cargo
check, without `filesystem-local` or `mcp-http` selected. A live foreign Cargo job
and 100% CPU observation initially
deferred further compiler overlap; after a fresh 19% CPU observation with about
41 GiB free, the final rerun used one Cargo job. Its terminal result and absence
of its Cargo/rustc processes and direct children were verified. These are local
development restart controls, not host-death or final-source cross-platform
qualification. Host death, model-tool binding, formal/installed-artifact gates,
actual Chromium, owner review, full CI and
actual-main landing remain open.

### Native MCP actual host-death development checkpoint

The standalone consumer's test-only controller starts its exact executable as a
host through the existing `ProcessTree`, with a cleared environment and private
host-storage reference. Before dispatch, that host syncs inert restart references
to its owned storage directory. Those references are outside the approved native
request and its environment. The approved peer initializes, applies the exact
Unicode/full-width tool arguments, writes physical output and one call-log entry,
then withholds its response. The controller has a fifteen-second readiness bound;
the admitted capture has a thirty-second bound for this cut.

After observing application, the controller terminates the contained host and
checks its failure exit status and the process owner's `is_reaped` state. Windows
Job cleanup covers the owned host and descendants under the existing containment
assumptions. Fresh local filesystem and stream instances reconstruct the admitted
task and native provider. A bounded stream read verifies exactly one native
attempt and one launch record, including its task/command and exact approval
digest. Recovery returns indeterminate and changes neither that record nor the
single call log; after the physical output is removed it creates no new write.
The reopened SDK destination has no published output.

Cancellation rejects fresh dispatch with `Conflict` while the exact old lease
still passes reconciliation ownership. A different placement must return
`Unauthorized`. Reconstructing the same recovered effects after cancellation
still reconciles the native attempt as indeterminate, with no terminal task
outcome or observed native receipt. The public ownership check requires a current
reservation, so this also checks that the old settlement lease was retained.
Memory, disk and cancelled-host recovery share one ordinary authority/provider
composition; no process engine or registry implementation was added.

Development controls caught three assertion defects during construction: the
temporary path needed the native view's canonical spelling, `terminate_after`
had already released the reaped child before a redundant `try_wait`, and a
foreign placement returns `Unauthorized` rather than the cancellation phase's
`Conflict`. The repaired final source passes both positive consumers, five memory
fault controls, six disk cases and actual host death on Windows. Strict
library/test/example Clippy passes without new suppressions, and the public
example still compiles with only `native-execution`. Terminal command results and
absence of the owned Cargo/rustc and example host/peer processes were verified.
These are development receipts, not the final cross-platform, formal, Chromium,
installed-artifact or full-CI qualification. Model-tool binding, owner review and
actual-main landing remain open.
