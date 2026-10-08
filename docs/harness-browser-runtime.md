# Slice I: portable ordinary-task execution

Initial inspected main: `4e3ed22bdc2137a93cccdd66a3cbb815acf897cc`, tree
`845c26a40bce308aed750ef71e4369069e622e4c`; rebased onto main
`8b547b48ca` after the Filesystem boundary landings, then `5d2336e299` after
shared durability/tracing and qualification updates, and `08a5b8ea79` after
Rust-owned policy initialization and Objects/Stream codec-frame changes.
Work is unqualified until
final-source execution and landing evidence replaces the pending entries below.

| Invariant | Production mechanism | Assumptions | Verification | Evidence |
|---|---|---|---|---|
| Browser tasks execute without a Tokio runtime | Existing `TaskGroup`, platform join adapter using `wasm_bindgen_futures::spawn_local`; same group semaphore/cancellation tree | Browser event loop available; live stack is not durable | Actual Chromium execution of a registered non-Send task, cancellation and capacity release without LocalSet | `target-i-rust-browser-run-2.log`: one Rust integration test passed; final-source rerun required |
| Caller deadlines and waits preserve total duration | Existing task limits, platform timers; signed-32-bit timer chunks preserve complete duration | Host timer/clock are trusted; suspended tabs provide no liveness guarantee | Native/browser timeout, cancellation and worker timer execution | Pending |
| Exact sender intent precedes receiver acknowledgment | Original intent stream retains exact sender/recipient/payload/revisions; one Stream commit appends receiver bytes and receipt pointer, plus the existing owner fence for owned send | Same admitted Stream provider; authority and payload availability remain valid; message operation IDs are globally unique | Changed content, mixed concurrent routes, lost acknowledgment, restart and stale lease tests | Intermediate native worker suite passed; expanded negative controls and final source pending |
| No uncertain effect replay | Existing task command host, stock model/tool resolver and workflow/execution journals | Provider reconciliation capability declared; unknown remains indeterminate | Real provider dispatch/receipt faults and restart | Pending affected-source execution |
| No browser scheduler or store | Existing `FilesystemTaskRuntime`, task registry/accounting, BrowserStream and real IndexedDB Filesystem providers; generated browser binding calls ordinary runtime methods | IndexedDB durability/quota/platform semantics; no cross-provider atomicity | Chromium provider tests, reload, two tabs and worker; installed composition pending | Six owning-provider Rust tests passed in `target-i-rust-browser-run-2.log`; worker termination/concurrent-tab/duplicate-wake/reopen passed in `target-i-browser-multitab-2.log`; final-source rerun required |
| Model observation differs from consumption | Mail publication retains durable delivery; retained request/consumption cursor belongs to ordinary workflow/request journals | PR6 supplies topology and consumption policy | Consumer qualification after PR6; no simulated swarm | Deferred full-default fork/merge composition to PR6/8 |

## Reuse and deletion audit

The platform adapter owns only task executor and timer operations. It creates no
task IDs, registry, scheduler, journal, durable state or admission policy. Native
tasks retain Tokio joins; browser tasks use abortable futures and completion
channels under the existing `TaskGroup` lifecycle. Replaced direct runtime timer
and local-executor calls are removed. Completion streams use the existing
platform-aware stream type.

Both mail publication paths use one atomic receiver publication and receipt
append in the original intent stream. The replaced mailbox history scan is
removed. Exact retries read the pointer and verify the receiver record, while
the first intent binds sender, recipient, payload and route/schema revisions.
Owned publication includes the existing coordinator condition and verifies its
lease before returning. A receipt confirms delivery, not effect settlement or
model consumption. There is no extra acknowledgment ledger or retry engine.

The existing IndexedDB authority/object providers and codec were moved into
`acyclic-fs` under an optional `browser` feature. `acyclic-fs-wasm` reexports them;
database schema, recovery, authority and publication fencing remain unchanged.
Harness does not depend on the unpublished bindings crate. Its package's
`./browser` entry reexports generated Rust runtime bindings and shared WASM
initialization. Host machines, tools and model callbacks are trusted explicit
providers, not confined code. Model events are pulled individually; the stock
executor owns admission and journals. Dropping the iterator aborts the supplied
signal; a host provider must honor cancellation for transport liveness.

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

Intermediate `target-i-native-mail-receipts.log` passed 234 unit and 20 actual
durable-local worker integration tests, including concurrent owned/unowned mail
publication, lost acknowledgment before checkpoint, provider reopen, stale leases,
cancellation and uncertain model/tool reconciliation. Later binding/clock edits
require affected tests to be rerun. `target-i-model-bindings-check-2.log` passed
the WASM target check before those clock/cancellation edits. Browser fixture
setup failures (unconstrained input schema, capability treated as a component
dependency) are retained in `target-i-browser-task-2.log` and `-3.log`; they are
not execution qualification. The later browser/build process handles disappeared
without terminal receipts; matching worktree processes were absent, so those
attempts provide no passing evidence. No PR or merge is claimed.

Later intermediate execution receipts: `target-i-native-mail-authority-3.log`
passed 235 unit and 20 durable-local worker tests, including sender and recipient
read-authority checks and exact sender/recipient/payload identity conflicts.
`target-i-browser-clippy-2.log` passed strict browser-target lint.
`target-i-rust-browser-run-2.log` passed one ordinary live-task Rust integration
test and all six moved IndexedDB provider tests in actual Chromium, driven
through the official wasm-bindgen test server. The first direct-glue attempt in
`target-i-rust-browser-run-1.log` failed because it omitted the runner's invocation
hook and startup suppression; it is retained and provides no passing evidence.

The concurrent-tab fixture initially exposed a physical Stream idempotency-key
collision between distinct envelopes for the same logical wake. Physical commit
keys now bind exact envelope bytes; the existing coordinator revision and logical
retained intent still own semantic deduplication. The failure is retained in
`target-i-browser-multitab.log`, and the affected fixture passed in
`target-i-browser-multitab-2.log`. This is bounded fault-test evidence, not a proof.

Native test dependencies are target-gated so they do not enable Mio on WASM.
Memory-only unit fixtures are feature-gated with `memory`; their assertions remain
unchanged and the default native suite must pass before final qualification.

`target-i-filesystem-native-tests-1.log` passed the pre-rebase default native
Filesystem suite: 1,150 passed, zero failed, 36 ignored host-dependent tests.
The two process-crash/power-loss core-store tests and million-path admission test
ran to completion; their long-running notices were not treated as failures or
grounds to restart. This receipt preserves native fixture coverage but needs
replacement after the shared main Filesystem changes.

On the rebased runtime, `target-i-browser-build-6.log` regenerated shipped WASM
and declarations. `target-i-browser-rebased-1.log` passed ordinary timer reload
and concurrent-tab/worker recovery, but failed the new tool fixture's incorrect
assertion that uncertain work must have no outcome. `Outcome::Indeterminate` is
the existing nonterminal reconciliation state. The corrected fixture passed in
`target-i-browser-tool-2.log`: a localStorage-backed tool effect executed once,
lost acknowledgment, reopened the real providers, reconciled the exact original
invocation once, and remained unchanged on duplicate wake.
`target-i-browser-model-1.log` passed canonical model request binding, retained
observed prefix, AbortSignal/iterator cleanup, provider reopen and reconciliation
without a second generation call. These are concrete bounded fault tests; final
Rust-derived worker/lease/result contracts and installed consumers are still
being qualified.

`target-i-native-rebased-2.log` passed 236 Harness unit tests and 20 durable
worker integration tests on the `5d2336e299` base. Browser artifacts built in
`target-i-browser-build-8.log` and `-10.log` add Rust-derived worker, retained
lease, admission, tick, outcome and inbox types. Only admitted FileRef lengths
use the existing exact Number projection; lease schemas and model literals are
not structurally rewritten. `target-i-browser-contracts-2.log` passed mail,
tool and model recovery, including a descriptor-shaped schema literal and the
typed inbox FileRef. Map declarations use Tsify's object representation to match
the existing browser serializer.

An isolated tarball consumer imported `@acyclic-labs/harness/browser` and ran
an ordinary registered task in `target-i-installed-browser-run-1.log`; strict
TypeScript checking, including generated declarations, passed in
`target-i-installed-browser-types-2.log`. Fake IndexedDB there qualifies package
resolution/composition, not browser storage durability; Chromium receipts above
cover the actual platform provider. These receipts precede the final outcome
and inbox declaration additions and the later main policy-boundary landing.

The bounded identity check uses the production `MailEvent` and canonical codec,
with two choices for sender, recipient, message ID, schema revision, route
revision and payload: 64 distinct canonical intents. Removing any one field is
a negative control that aliases the domain to 32 identities. It checks exact
byte binding and decode/redelivery within that finite domain. Revision 2 probes
binding only and is not a supported inbox revision. It makes no claim about
unbounded traces, hash injectivity, transport liveness or storage atomicity;
provider and restart tests supply separate implementation evidence. The initial
compile failure in `target-i-mail-bounded-model-1.log` is retained; the repaired
check passed in `target-i-mail-bounded-model-2.log` (one bounded-domain test,
236 other tests filtered). The repaired browser lint still requires a passing
receipt after main reconciliation.

The current-main refresh passed strict browser library lint in
`target-i-browser-clippy-5.log`. Main's UTF-16 validation now compares the
browser's numeric code units directly against surrogate ranges, without lossy
integer casts. The browser integration additionally tests a real scoped task
deadline and destruction of its pending future; its execution receipt remains
pending. The combined browser driver includes main's initialization-failure and
retry page.

`target-i-typescript-main273-2.log` passed all 241 Harness TypeScript tests after
rebuilding the changed Objects WASM/TypeScript dependency. The prior 240-pass,
one-failure receipt is retained in `target-i-typescript-main273-1.log`; the
failed Objects consumer used stale bindings missing the new body decoder.
Browser outcome declarations now reuse `WasmModelJsonValue`, which admits the
existing bigint integer representation. An installed-consumer assignment of
`Succeeded: 7n` checks that declaration rather than relying only on runtime
equality.

The inspected macOS test host has 372 MiB free. No slice I build was dispatched
there, and no other owner's files were removed. macOS execution, the independent
B model-dispatch seam, installed full-package gates and final landing remain
open; intermediate passes do not establish readiness.
