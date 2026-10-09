# Portable ordinary-task execution

The browser adapter uses the existing `AgentHarness`, task and machine
registries, scheduler, admission records and durable journals. Rust owns task,
model and tool semantics. JavaScript provides trusted machine implementations,
model/tool transport callbacks, browser timers and IndexedDB operations.

## Public composition

`@acyclic-labs/harness/browser` reexports generated Rust bindings and the shared
WASM initializer. Construct an authority, register explicitly selected task and
tool definitions, and open `BrowserTaskRuntime` with Filesystem and Stream
providers. Opening a runtime grants no authority and dispatches no work.

Use `admit` for the original operation, `runOperation` to execute only that
task, or `workerTick` for caller-bounded owner-wide discovery and execution.
An unavailable exact target never substitutes another task. `recoverWork`
reads an existing reservation without claiming, releasing or dispatching work;
`resume` verifies the supplied original attempt against the current durable
fence. Cancellation can leave an unresolved reservation that still requires
reconciliation. `outcome` only observes retained state.

Model generation receives canonical prepared-request bytes, the landed
`ModelDispatch` identity and an `AbortSignal`. The stock executor owns request
admission, event bounds, accounting and recovery. Transport callbacks do not
select another request or redispatch an uncertain effect. Tools use the same
retained invocation during execution and reconciliation.

## Invariants and assumptions

| Invariant | Production mechanism | Assumptions and verification |
|---|---|---|
| Ordinary tasks have the same lifecycle on both targets | Existing `TaskGroup`, cancellation tree and capacity semaphore; platform join/timer adapter | Native futures remain `Send`; browser futures may be local. Live stacks are not durable. Rust browser tests exercise cancellation, deadlines and descendant closure. |
| Exact admitted work survives uncertain delivery | Coordinator reservation and original workflow/execution journals | Providers expose reconciliation; unknown outcomes remain indeterminate. Browser effect fixtures interrupt delivery and reopen providers. |
| Recovery creates no execution authority | Authenticated admission lookup, original reservation and current fence verification | `recoverWork` is read-only; `resume` still authenticates ownership. Native fault tests and browser malformed-receipt recovery inspect the original attempt. |
| A targeted run cannot consume unrelated work | Existing scheduler readiness predicates and exact-operation pull | The browser consumer admits a lower-ordered unrelated task and verifies that it remains unclaimed. Terminal retries produce no work. |
| Durable delivery precedes acknowledgment | Canonical mail intent plus one Stream commit for receiver bytes and receipt pointer | Same admitted Stream provider and available authorized payload; globally unique message operation identities. Mail tests cover exact redelivery and mismatched payloads. No cross-provider atomicity claim. |
| Wakeups do not replace durable state | Existing BrowserStream, Filesystem authority and coordinator conditions | IndexedDB durability and quota semantics apply. Concurrent tabs and terminated workers exercise stale ownership and reopen. |
| Schemas and literal values keep their meaning | Rust-derived declarations and bigint-capable model JSON contracts | Only admitted `FileRef` byte lengths use the existing exact Number projection; schema/model objects are not structurally rewritten. |

The finite mail identity check uses the production canonical codec over 64
combinations of sender, recipient, message, schema revision, route revision and
payload. Removing any field aliases 32 identities. This is bounded identity
evidence, not a proof of hash injectivity or transport liveness.

Machine registration checks the existing exact task key before insertion.
Only `NotFound` denotes vacancy. Exclusive synchronous access prevents another
registration between the check and insertion; schemas are validated once.
Browser consumers exercise invalid-schema retry and occupied-key rejection
without invoking machine callbacks during registration.

The IndexedDB authority/object providers live in `acyclic-fs` behind its
optional browser feature. `acyclic-fs-wasm` uses those same providers; Harness
does not depend on the bindings crate. No separate browser scheduler, task
registry, acknowledgment ledger or effect retry engine exists.

## Qualification scope

Qualification must identify the exact source, generated JS/WASM pair,
declarations and installed package bytes. Run affected native, WASM, Chromium
reload/multitab/local-and-HTTP routing, installed-consumer and hosted checks.
Historical receipts are preserved as artifacts and do not qualify changed
source. Current requalification is in progress; the PR and final landing
receipt establish the qualified revision.

The HTTP test provider durably records external intent/results, loses a
response and reconciles the exact invocation. It is test infrastructure, not
an SDK transport policy or a server-crash guarantee.

Native execution and stdio require optional providers. Persistent stock-task
deadline composition remains explicitly unsupported. Suspended tabs have no
liveness guarantee, and transports must honor cancellation for transport
liveness. Full portable default tools, canonical root-turn composition and
recursive steering remain separately tracked requirements; these adapters
alone do not establish completion of the full slice I goal.


`WasmTaskRegistry.registerStockTurn()` registers the same Rust stock-turn definition as native callers. Its input is the existing model command's operation ID, kind and immutable payload reference. It uses the existing model outbox and recovery path; registration grants no authority and starts no work.

`configureModel` accepts optional synchronous capacity and token-counting callbacks after generate/reconcile. Capacity receives the exact model selection. Counting receives the exact canonical request bytes and Rust request digest; its returned `ModelTokenCount` must bind that digest and contain one additive upper bound per ordered message. Rust applies the canonical capacity and count validators. These trusted provider callbacks must be effect-free, and promises, malformed bounds and foreign request counts are rejected. Missing callbacks retain the explicit unsupported result. Default compaction still requires its separately qualified context pipeline consumer.
