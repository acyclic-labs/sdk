# Slice I: portable ordinary-task execution

Inspected main: `4e3ed22bdc2137a93cccdd66a3cbb815acf897cc`, tree
`845c26a40bce308aed750ef71e4369069e622e4c`. Work is unqualified until
final-source execution and landing evidence replaces the pending entries below.

| Invariant | Production mechanism | Assumptions | Verification | Evidence |
|---|---|---|---|---|
| Browser tasks execute without a Tokio runtime | Existing `TaskGroup`, platform join adapter using `wasm_bindgen_futures::spawn_local`; same group semaphore/cancellation tree | Browser event loop available; live stack is not durable | Native cancellation regression; actual Chromium execution without LocalSet | Pending execution; WASM+Filesystem compilation passed in `target-i-wasm-check-2.log` |
| Caller deadlines and waits preserve total duration | Existing task limits, platform timers; signed-32-bit timer chunks preserve complete duration | Host timer/clock are trusted; suspended tabs provide no liveness guarantee | Native/browser timeout, cancellation and worker timer execution | Pending |
| Exact sender intent precedes receiver acknowledgment | Existing Stream keyed publication and committed-byte verification | Same admitted Stream provider; authority and payload availability remain valid | Changed recipient/content/revision, lost acknowledgment, restart and stale lease tests | Implementation incomplete for owned-send path |
| No uncertain effect replay | Existing task command host, stock model/tool resolver and workflow/execution journals | Provider reconciliation capability declared; unknown remains indeterminate | Real provider dispatch/receipt faults and restart | Pending affected-source execution |
| No browser scheduler or store | Existing `FilesystemTaskRuntime`, task registry/accounting, BrowserStream and real Filesystem providers | IndexedDB durability/quota/platform semantics; no cross-provider atomicity | Public installed browser composition, reload, two tabs and worker | Pending binding/provider seam |
| Model observation differs from consumption | Mail publication retains durable delivery; retained request/consumption cursor belongs to ordinary workflow/request journals | PR6 supplies topology and consumption policy | Consumer qualification after PR6; no simulated swarm | Deferred full-default fork/merge composition to PR6/8 |

## Reuse and deletion audit

The platform adapter owns only task executor and timer operations. It creates no
task IDs, registry, scheduler, journal, durable state or admission policy. Native
tasks retain Tokio joins; browser tasks use abortable futures and completion
channels under the existing `TaskGroup` lifecycle. Replaced direct runtime timer
and local-executor calls are removed. Completion streams use the existing
platform-aware stream type.

Both mail publication paths retain the receiver scan until an atomic durable
receipt lookup is integrated. Different keyed append/lease-fenced commit routes
must not race to append the same logical record twice. This is a known
remaining simplification, not an established bounded-work claim. Its lease/CAS
checks must remain intact. Sender intent must cover both routes before acceptance.

Real Filesystem browser stores currently reside in `acyclic-fs-wasm`; their public
Rust reuse must preserve package closure without importing the complete bindings
crate or copying the provider. The exact owning seam is under coordination.

## Receipts and claim boundaries

`target-i-wasm-check.log` failed in the Windows sandbox because the bundled
protocol compiler could not load DLLs (OS error 623). The unsandboxed retry
`target-i-wasm-check-unsandboxed.log` found an owned-send integration compile
failure, which was repaired without removing its fencing checks.
`target-i-wasm-check-2.log` passed compilation of the real Harness with
`wasm,filesystem` for `wasm32-unknown-unknown`. Compilation is implementation
evidence only; it does not establish browser execution, durability, liveness,
fault correctness, installed consumer usability or formal correspondence.

All final-source production, fault, bounded formal, Windows, Linux/WSL, macOS,
Chromium/WASM, generated and installed-consumer receipts remain required. No
unrestricted proof, confinement, native mount or cross-provider atomicity claim.
