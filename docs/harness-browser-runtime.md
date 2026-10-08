# Slice I: portable ordinary-task execution

Base main: `08a5b8ea796a3b85aeee3e56a79d0439ae68247f`. Draft PR:
https://github.com/acyclic-labs/sdk/pull/274. This is a qualification record,
not a readiness or landing claim. Receipts below identify their source scope;
the latest source still requires final native/installed checks and macOS execution.

## Production mechanisms and boundaries

| Invariant | Mechanism | Assumptions and limits |
|---|---|---|
| Same ordinary-task semantics in native and browser runtimes | Existing TaskRegistry, AgentHarness, TaskGroup, scheduler/accounting and workflow/execution/conversation journals; platform executor/timer adapters | Native futures remain Send; browser futures may be local. Live task stacks are not durable. |
| Cancellation releases task capacity and destroys pending work | Existing cancellation tree and semaphore; browser abortable futures and completion channels | Browser event loop available. Provider transports must honor AbortSignal for transport liveness. |
| Caller deadlines preserve total duration | Existing task limits; browser timers use signed-32-bit chunks and retain the full duration | Trusted host clock/timers; suspended tabs have no liveness guarantee. Persistent worker/stock-task deadline composition remains explicitly Unsupported. |
| Exact intent precedes receiver acknowledgment | Original intent stream binds sender, recipient, exact payload and schema/route revisions; one Stream commit appends receiver bytes and receipt pointer | Same admitted Stream provider; valid authority and payload availability; globally unique message operation IDs. No cross-provider atomicity claim. |
| Exact retries do not redispatch uncertain effects | Existing task command host, stock model/tool resolver and retained workflow/execution journals | Provider reconciliation capability is explicit; unknown remains indeterminate. Receipt confirms delivery, not model consumption. |
| Fenced browser recovery | Existing BrowserStream and IndexedDB Filesystem providers, ordinary coordinator owner conditions and retained leases | IndexedDB durability/quota semantics; transport and wakeups do not own semantic state. |

The platform adapter creates no task IDs, registry, scheduler, journal or
admission policy. Native tasks retain Tokio joins. Browser tasks use
wasm_bindgen_futures::spawn_local under the existing TaskGroup lifecycle.
Replaced direct timer/local-executor calls are removed.

Mail publication removes the previous mailbox history scan. Exact retries read
the receipt pointer and verify the receiver bytes. Owned publication includes
the existing coordinator condition and checks the lease before returning.
There is no extra acknowledgment ledger, mailbox registry or retry engine.

The existing IndexedDB authority/object providers and codec moved into
acyclic-fs under its optional browser feature. acyclic-fs-wasm reexports those
same providers; database schema and publication/recovery fencing are unchanged.
Harness does not depend on the unpublished bindings crate. Its browser entry
reexports generated Rust bindings and shared WASM initialization.

Machines, tools and models are explicit trusted host providers. There is no
default native process/filesystem provider or confinement claim. Model events
are pulled individually, with admission and journals owned by the stock
executor. B's independent signed fbaea4d355 dispatch seam is integrated without
its unfinished ModelPurpose/context work. Generation receives canonical request
bytes, a separate dispatch identity and AbortSignal. The callback selects
operation_id, step and request_digest from existing WasmModelAttemptWire;
dispatch metadata is not inserted into the canonical request.

Public worker, lease, tick, outcome, inbox and option declarations derive from
Rust. Generic values and schema literals reuse the existing bigint-capable
WasmModelJsonValue/WasmModelJsonSchema types. Only admitted FileRef lengths use
the existing exact Number projection. Lease schema/model literals are not
structurally rewritten. Maps use the existing serializer's object representation.

Full-default topology, steering and durable consumption policy remain PR6/8
composition, through ordinary task/request journals. This slice supplies
transport/provider adapters and does not simulate a complete swarm.

## Qualification receipts

| Source scope | Check | Receipt |
|---|---|---|
| a3cc66123d | Windows strict native all-target lint; 237 Harness unit and 20 durable-worker tests | target-i-native-dispatch-clippy-2.log; target-i-native-dispatch-2.log |
| Frozen a3cc66123d archive | Linux/WSL strict lint; 237 unit and 20 durable-worker tests | target-i-linux-candidate-a3cc66123d-3.log |
| a3cc66123d packaged closure | 291 Rust tests, 47 installed TypeScript tests, seven hashed artifacts | target-i-installed-full-candidate-3.log; target/i-harness-package-candidate/CONFORMANCE-EVIDENCE.json and SHA256SUMS |
| Current declaration correction | Actual tarball strict TypeScript assignment of bigint schema literals and Succeeded: 7n; installed browser task execution | target-i-installed-browser-types-final-2.log; target-i-installed-browser-types-final-run-2.log |
| Current Filesystem test-guard repair | Strict all-target/all-feature Filesystem lint | target-i-filesystem-all-feature-clippy-1.log |
| Current owning Filesystem browser source | Six actual Rust IndexedDB tests in Chromium, zero ignored | target-i-fs-browser-final-1.log |
| Current generated browser artifact and fixtures | Eight Chromium pages: wire/discovery, initialization retry, timer reload, worker termination/tab fencing, local and HTTP tool recovery, model dispatch/reconciliation, mail faults | target-i-browser-final-contract-1.log |
| Production canonical mail intent, included in native/installed suites above | 64 intent combinations and six field-removal negative controls | target-i-mail-bounded-model-2.log |

The finite mail check uses production MailEvent and the canonical codec: two
choices each for sender, recipient, message ID, schema revision, route revision
and payload yield 64 distinct intents. Removing any one field aliases the
domain to 32 identities. Revision 2 probes binding only and is not a supported
inbox revision. This is bounded identity/codec evidence, not an unrestricted
trace proof, hash injectivity proof or transport-liveness proof.

The HTTP fixture is an explicit test provider. It fsyncs external-effect intent
and result files, deliberately loses the response, and reconciles the original
exact invocation after browser runtime reopen. Changed content is rejected;
inspection confirms one execution. These files do not add an SDK journal or
production transport policy. This receipt does not prove server-crash recovery
or power-loss durability of directory entries.

The losing concurrent admission fixture verifies the existing retained
Conflict attempt: no machine transition occurs, and resuming its stale fence
still fails. Exactly one winner, durable completion, duplicate-wake suppression
and reopen remain required. Pure machine transitions may be evaluated again
during validation and do not establish an external-effect dispatch count.

Failed receipts are retained. The first Linux native codegen run hit an LLVM
stack fault; the passing retry used rustc's recommended RUST_MIN_STACK=16777216
for compiler threads, without changing production resource limits.
PR run 37709699301 passed gate and TypeScript jobs but failed policy; Windows
was skipped. The policy failure arose from Clippy not recognizing combined
test/feature guards. Separate equivalent cfg attributes preserve assertions
and pass strict Filesystem lint; a fresh pushed-source CI run is still required.

The actual browser parent/descendant cancellation test is currently compiling.
Final-source native, WASM lint, generated and full installed-consumer receipts,
successful required CI, macOS and landed-tree verification remain open.
The inspected macOS host ivar has about 316 MiB free; no slice I build was
dispatched and no other owner's files were removed. No unrestricted proof,
native mount, confinement or cross-provider atomicity claim is made.
