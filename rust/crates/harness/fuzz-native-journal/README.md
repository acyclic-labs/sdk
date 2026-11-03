`native_receipt` calls the public `host_process::Receipt::{accepted,confirm}`
used by native process admission. Its oracle shares `AgentPath`'s parser and
checks only the receipt contract: confirmation retains the admitted task,
refuses another task before reading its view, and returns the current view
for the original task. Arbitrary JSON and sequences exercise failed admission,
missing and malformed fields, foreign identities and view remounts.

The same oracle is used by `tests/native_receipt.rs`, which creates an actual
native workspace owner, two sibling sessions and a nested child, retires the
owner and reopens its persisted sessions. It prints the original serialized
reply sequence as `NATIVE_RECEIPT_CAPTURE`; those SDK-produced bytes supply the
fuzzer corpus. Derived field mutations are not captured external responses.
This checks receipt admission, not native CLI execution, distributed serving,
process cancellation or the physical execution fence.

The retained `corpus/native_receipt/owner-identities` projects the five replies
captured by that control at commit `b52cd96d452abb50793f8fb47cf8b433feb24bc6`
on Linux x86_64. Original serialized capture SHA256:
`3aed01d936eed113c1eea9fe27195a2d94e0361ef906d30146b8565d5b48a535`.
Only the original `task` values remain unchanged; private working directories
and unrelated reply fields are omitted. No replacement wire values are claimed.
`derived-views` adds explicitly mutated empty/current views, wrong field types
and an absent task to those original identities. The other two derived seeds
exercise admission with an absent task and a null task.

| Seed | SHA256 |
| --- | --- |
| `owner-identities` | `38eac08b5e0e18f1a4a6f2dbecbb304c1552adcd9716a3d1b5cf045cf09ebbff` |
| `derived-views` | `12803fad195012bf2b5fac2b60009749de7f5bef7e38a97671bb587213a8460b` |
| `derived-task-absent` | `44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a` |
| `derived-task-type` | `f7d147326d8223b27b438cdcae69640fc76859270960341bec3fced6644b0fee` |

Run the producer control with
`cargo test -p acyclic-harness --no-default-features --features filesystem-local,native-execution --test native_receipt -- --nocapture`.
Run the retained target with
`cargo fuzz run native_receipt --fuzz-dir rust/crates/harness/fuzz-native-journal`.
Coverage must include the called harness library; instrumenting only this
target does not establish production receipt coverage. A planted removal of
the task comparison in `Receipt::confirm` must fail the unchanged oracle with
`foreign task passed native receipt confirmation`.

`native_child_reference` calls the original `host_workspace::child_reference`
parser used before native spawn commits and mount registration. Its shared
test-support oracle checks admitted inputs against actual path components and
the portable mount alphabet and bounds invalid-input diagnostics. A separate
generator requires valid components to be admitted, so a reject-all parser
cannot pass. Arbitrary input is not
truncated; generated components obey the existing 255-byte filesystem bound.
This checks reference syntax, not mount confinement or process lifetime.

The corpus contains explicitly authored user references, including `first`
(the input used by the real-owner receipt control), parent/current paths,
both separators, non-ASCII input and the component-size boundary. These are
request inputs, not recorded external responses. The retained mutation
patches remove parent refusal, force reject-all admission, and restore the
original diagnostic that copied the entire rejected input. Each must fail
the unchanged oracle at its corresponding assertion; none is a compiler
failure control.

The native ready-queue control
`local::host_process::children::framing::only_fired_registrations_reach_the_ready_queue`
uses the same production-queue oracle as `native_readiness`: arbitrary
sequential schedules poll either waiter, fire owned or borrowed registrations,
and consume pending keys. The existing unit schedule retains its 1,000
registrations; fuzz schedules use 16 participants. The oracle compares exact
membership and notification counts after each action, including waiter
replacement, duplicate coalescing, early readiness and re-registration.
The retained `mutations/native_ready_notification.patch` drops the production
notification while preserving queued keys. The unchanged control must fail
at `fired child registration did not wake the current waiter exactly once`.
`mutations/native_ready_membership.patch` substitutes an unregistered key and
must fail at `native ready queue lost, invented or repeated a child notification`.
These are mutations of the actual queue, not of an alternate implementation.
The corpus consists of authored internal action schedules, not external wire
captures: `replacement` is the existing unit schedule, `multiple-children`
queues several recipients before consumption, and `empty-consumption` checks
that consuming an empty queue does not discard its waiter. In each action byte,
the top two bits select poll, borrowed wake, consume, or owned wake; low bits
select the waiter or child. Run with
`cargo fuzz run native_readiness --fuzz-dir rust/crates/harness/fuzz-native-journal`.
This is a synchronous component check, not an OS event-hub concurrency proof.

The internal schedule seed digests are:

| Seed | SHA-256 |
| --- | --- |
| `replacement` | `6dbc0be31823b21866b3802571ae94b848a87d2a6a082b9245f2af14450d75b5` |
| `multiple-children` | `a50938f0d40fbb111d5ecb88c5b9a86af5455a517f0823651089bac780422a05` |
| `empty-consumption` | `2aab45833c58f6165ff8162229607ccfae4e0ebe8af94afc879ea1a33f67558f` |
