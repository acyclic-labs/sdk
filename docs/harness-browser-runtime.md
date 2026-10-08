# Slice I: portable ordinary-task execution

Original base main: `08a5b8ea796a3b85aeee3e56a79d0439ae68247f`. Draft PR:
https://github.com/acyclic-labs/sdk/pull/274. This is a qualification record,
not a readiness or landing claim. Receipts below identify their source scope;
the latest source still requires final qualification after main integration.

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
| Signed 881bbef3bf, including main b94284f885cf | Windows strict native lint, 240 unit and 20 durable-worker tests; strict Filesystem/Harness WASM lint | target-i-main-881bbef3-native-clippy-1.log; target-i-main-881bbef3-native-1.log; target-i-main-881bbef3-fs-wasm-clippy-1.log; target-i-main-881bbef3-harness-wasm-clippy-1.log |
| Exact 881bbef3bf archive | Linux strict native lint, 240 unit and 20 durable-worker tests; native declarations match | target-i-linux-main-881bbef3-1.log |
| Exact 881bbef3bf archive through ssh ivar | macOS strict native lint, 240 unit and 20 durable-worker tests | target-i-macos-881bbef3-1.log; target/i-macos-881bbef3-receipts |

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

On ac36f87145, the two actual Rust Harness browser tests passed in
target-i-harness-browser-final-1.log. Windows and Linux each passed strict native
lint, 237 unit tests and 20 durable-worker tests; strict WASM lint also passed.
The exact installed closure passed 291 Rust and 47 TypeScript tests, with seven
verified artifact hashes and SOURCE_COMMIT ac36f87145, in
target/i-harness-package-ac36f87145. A strict consumer of that exact tarball
passed bigint schema/outcome checking and browser execution. Automatic PR run
37712929368 succeeded; forced complete matrix 37714072774 is still running on
that source and does not qualify the subsequent timer correction below.

The production scheduler test checks all 40,320 orderings of eight inputs over
three tasks, two attempts and two leases. It includes changed-request and
foreign-fence negative controls, cancellation and retained uncertainty. These
bounds are a test scenario, not runtime ceilings. The native context measurement
uses 1, 16 and 128 retained revisions: each latest lookup verifies one reference
and returns 547 canonical bytes (target-i-bounded-history-final-1.log).
The measured latencies include a cold first lookup and are not a latency bound
or whole-runtime memory measurement.

The completion audit found that durable timer polling still scanned every
earlier timer for the task. Timer records now use the existing Stream journal
scoped by task and operation, with exactly one canonical identity/deadline
record. The task-wide scan is removed; there is no additional timer ledger or
lifetime timer-count ceiling. The new test measures one read of at most one
record after reopen with 1, 16 and 128 retained timers, and rejects a changed
deadline. Existing integration checks still retain all 72 independent timers,
concurrent exact retry, changed-deadline rejection and stale-owner denial.
This changes the supported timer journal layout; the prior task-wide layout
has no compatibility fallback. Strict native all-target lint and the focused
measurement passed in target-i-timer-bounds-clippy-2.log and
target-i-timer-bounds-2.log (one read, maximum one record for all three retained
counts). The initial fixture type-inference failure is retained in the first
lint receipt. Signed source 0542ee06c4 then passed Windows and Linux strict
native lint, 238 unit tests and 20 durable-worker tests, and strict Harness WASM
lint (target-i-native-timer-final-1.log,
target-i-linux-timer-final-1.log, target-i-browser-timer-final-clippy-1.log).
All eight actual Chromium pages passed in target-i-browser-timer-final-1.log.
The installed closure passed 292 Rust and 47 TypeScript tests with seven
verified artifact hashes and SOURCE_COMMIT 0542ee06c4 in
target/i-harness-package-0542ee06c4 (target-i-installed-timer-final-1.log).

macOS capacity subsequently became available. The exact 0542ee06c4 archive,
SHA256 a8c895fd68afc08de4901d4813efd38a0fe22159429e912e656791f4eb15a8c8,
passed strict native lint, 238 unit tests and 20 durable-worker tests through
ssh ivar in the isolated /tmp/sdk-slice-i-0542ee06c4 directory.
target-i-macos-0542ee06c4-1.log and
target/i-macos-0542ee06c4-receipts retain the raw lint/test logs and source
hashes. The slot was released to PR5 after this build completed.

Automatic PR run 37715110202 succeeded on 0542ee06c4. The older forced matrix
37714072774 exposed a filesystem-WASM private parser borrow lint and a Linux
native declaration failure. Fresh Windows and Linux declarations, including
CI's Bun 1.4.2, matched the committed file. A negative control reproduced
missing FileKind/FilePayloadKind declarations when the cached filesystem
dependency lacked its external macro output
(target-i-linux-napi-missing-dependency-1.log). Declaration generation now
checks both crates' output, rebuilds missing output, and bypasses compiler
wrappers for this macro-emission build; the filesystem build tracks its
declaration environment. Windows generation matches the committed declaration
(target-i-windows-napi-repair-1.log); strict all-feature filesystem lint passed
(target-i-filesystem-build-script-clippy-1.log). Linux recovery qualification
passed all three cached-output scenarios in target-i-linux-napi-repair-1.log:
missing dependency output, warm generation and regeneration after a native
build without declaration output.

Signed merge 881bbef3bf integrated main b94284f885cf without conflicts. The
exact source archive has SHA256
d56c2018b146084a1424fdfab1cc0092c9a29df50628b232aec5d8fccc1ea105.
Its Windows, Linux and macOS native suites passed 240 unit tests and 20
durable-worker tests each, with zero ignored; both strict WASM checks passed.
The copied macOS receipt includes hashes of executor, execution journal,
durable host and task-workflow sources matching this worktree. Only owned
package build outputs were reclaimed after the job completed; raw receipts
and source were preserved, and the slot was released to PR5.

Automatic PR run 37720079533 succeeded on 881bbef3bf. Forced full run
37720933376 failed before any lanes: the manual feature-branch signature
check uses the merge's first-parent range, which includes the already-landed
GitHub web-flow signature on b94284f885cf. PR preflight verified the owned SSH
commits against its merge base. Signature policy is unchanged; the next
signed documentation checkpoint records these receipts above the merge.
The first installed check stopped at a missing locked Node type dependency
(target-i-installed-881bbef3-1.log); it does not qualify the installed closure.
Final browser/installed qualification, successful complete CI and
landed-tree verification remain open. No unrestricted proof, native
mount, confinement or cross-provider atomicity claim is made.

The installed 0c78244871 closure subsequently passed 294 Rust and 47
TypeScript tests, with all seven artifact hashes verified
(target-i-installed-0c78244871-1.log; target/i-harness-package-0c78244871).
Forced full run 37722029026 passed Linux generation, installed consumers
and conformance, then failed protobuf lint on the established Objects and
Stream CODEC_NONE zero values. A rule-specific exception for those two
files preserves their valid unencoded-byte wire semantics; full local Buf
lint passes in target-i-buf-codec-lint-1.log. Other matrix lanes remain
pending at this checkpoint.

An additional recovery audit found that decoding a malformed host tool
receipt after its callback ran was classified as Invalid. The callback may
already have committed its effect, so that classification incorrectly
recorded terminal ToolFailed. Decode failures now retain uncertainty through
the existing Storage path, preserving the original attempt for reconciliation.
The real Chromium negative control on the original source never reached
reconciliation (target-i-browser-malformed-negative-2.log). The repaired
fixture passes malformed execution and reconciliation receipts, two reopens,
exact invocation recovery, one effect, one projection, model steps [0,1]
and a durable successful outcome (target-i-browser-malformed-positive-4.log).
This assumes an explicitly registered trusted provider with reconciliation;
it does not establish exactly-once effects for arbitrary external services.
