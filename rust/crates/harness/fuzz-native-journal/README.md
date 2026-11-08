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
`mutations/native_ready_poll.patch` inverts the production empty-queue decision
and must fail at `native ready queue parked with unconsumed notifications`.
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

`native_launch` enters `launch::inherited_arguments`, the JSON string-vector
admission called by `NativeChildren::open`, then the original `validate`,
`parent_flags` and `child_arguments` functions. It never changes process-wide
environment variables or starts a native host. Its inputs are authored root
policy requests in the same JSON-vector format that `run_root_session` writes;
they are not external-tool responses. An independent JSON value projection
checks exact vector contents and type refusal. The target also generates valid
root policies and checks unchanged inheritance, exact resume suffixes, required
policy cardinality, missing values, host-specific switches and option refusal.
The existing physical path/collision and argument-allocation controls remain;
their repeated policy-refusal examples are subsumed by this target.

Retained source mutations replace actual vector decoding with an empty vector,
remove unknown-option admission, and bypass the resume-option guard. They must
fail at `native policy decoder changed root argument vector`,
`native launch admitted an unknown policy option`, and
`native resume reference was admitted as an option`, respectively. A compiler
error or an unrelated panic is not a qualifying negative control. Run with
`cargo fuzz run native_launch --fuzz-dir rust/crates/harness/fuzz-native-journal`.
This checks the in-process format and policy boundary; it does not qualify the
OS environment handoff, native CLI parsing, credential inheritance, physical
processes, concurrency or all platforms.

The same target checks AGY's relative inline file-permission resolution through
the production child argument builder: allow, ask and deny keep their native
lists; relative read/write targets name the admitted cwd; absolute paths,
wildcard/home references, provider settings and unrelated fields are preserved.
Arbitrary external filenames are not opened by the in-memory target. The shared
launch property covers actual selected settings files and both root and child
invocations. `mutations/native_launch_relative_permissions.patch` disables
production relative-path resolution and must fail at `relative permissions
changed authority or workspace` in this target, or at the shared launch property.

The same target supplies relative and absolute configuration paths and requires
their inherited targets to remain rooted at the original workspace. It also
constructs native non-Unicode working directories from arbitrary input bytes
(invalid UTF-8 on Unix, an unpaired UTF-16 surrogate on Windows), without touching
the filesystem. Both relative configuration paths and the appended workspace
root must return the original typed refusal rather than a lossy replacement.
Retained mutations omit path anchoring and replace both Unicode checks with
lossy conversion. They must fail at `native path policy changed its root target`
and `native launch accepted a non-Unicode policy root`, respectively. The physical
collision control remains distinct: this target does not resolve or open paths.

The same path oracle checks that Pi package/URL references (including leading
whitespace in its package syntax), file URLs and home references keep their
original bytes instead of being anchored as local paths. Both extension and
session-directory options exercise their shared home/file exemptions. Native
Windows home separators are included on Windows. These are authored policy
requests, not recorded native CLI responses. The retained
`mutations/native_launch_package.patch` and `mutations/native_launch_home.patch`
disable the actual package and home/file exemptions independently. Each must
fail at `native Pi resource reference was rebased as a local path`; these checks
do not establish how Pi itself interprets or opens those references.

Generated non-path policy requests also compare their original argument bytes
with `parent_flags` before checking child inheritance. Claude settings contain
an arbitrary JSON string inside an authored object, with empty, space, tab or
newline prefixes. This input-to-parent relation covers inline settings and the
other generated policy values for all three hosts. It replaces the literal
inline-settings example in the physical path test. The retained
`mutations/native_launch_inline.patch` makes the original inline-settings
branch classify every value as a local path; it must fail at
`native launch changed non-path policy bytes`.

Fresh and resumed child commands use one complete-vector assertion, including
the native event-mode flags, Codex's cwd argument, inherited policy, resume
reference and stdin marker. The retained `mutations/native_launch_command.patch`
changes all three hosts' fixed event-mode arguments and the Codex cwd while
preserving policy placement, resume suffixes and allocation counts. The former
partial policy/suffix oracle accepts that defect; the complete-vector oracle
must fail at `native child changed the complete command vector`. This checks
argument construction, not native CLI execution or process cwd application.

The separate `native_child_arguments_allocate_only_the_returned_vector_and_strings`
unit measures actual allocation counts for fresh and resumed arguments of all
three hosts, including Pi's empty policy. It requires exactly one result vector
and one allocation per nonempty returned string. Argument-value assertions live
in the shared byte target instead of being repeated in this allocation control.
The retained `mutations/native_launch_allocations.patch` clones and drops the
actual completed argument vector; the unchanged allocation assertion must fail
at `native arguments must allocate only their nonempty strings and one result vector`.
The fault changes production work while preserving the returned arguments.

The retained request seeds and their SHA-256 digests are:

| Seed | SHA-256 |
| --- | --- |
| `codex` | `cb0700ac23729a7dbeba8332730b04edf39cc96599ec26f0ca218ac30c52ef46` |
| `claude` | `0a91eb423124a013cf930c5beb5e207d1c44b92ee27e17842c8eb20098b51bd6` |
| `pi` | `f08a6db6b04ea023bf0585d2881315d7f29fca1b422e5953ed4c68b7977ecc65` |
| `wrong-type` | `8a925a5f20c4df4fcd3ef7f313094fc68a107f8ee91af96cdcd2df5c95ba084f` |
| `duplicate-policy` | `87b8a1fb5c7e0eead885ca8153d8afd4f9444c48e80bea15b61dec7a8a08f06b` |
| `missing-value` | `dfa99913ec3738de2968c58d601846d4efc06a0cdf871c9ded10b536e6dcd6ca` |
| `empty` | `4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945` |
| `partial-json` | `795c797dd7cbb280e5d38dd4ce2ddc06d48fb65eed17b365311392c20c6c1f68` |

Native request presence is checked by the real-file custody control:

```sh
cargo test --locked -p acyclic-harness --features filesystem-local,native-execution --lib local::host_files::tests::request_observation_failure_preserves_accepted_work_until_real_withdrawal -- --exact --nocapture
```

It uses the production request serializer, immutable projection writer and
`Served` admission/dispatch/reply path. At queued, running and answered phases,
a real non-file request in the original held directory must retain accepted ownership, its original cause
and any published reply bytes; only an actual absent projection withdraws.
It also checks zero directory
rescans for named events. No decoder, filesystem-error model or alternate
completion producer is introduced. The retained `native_request_presence.patch`
restores the original error-as-withdrawal defect and must fail at
`observation failure was treated as withdrawal`; `native_request_scan.patch`
forces actual directory scans and must fail at
`named request observation scanned history`. These are component controls;
original caller/owner cancellation and physical retirement remain separate.

The retained `mutations/native_launch_current_directory.patch` preserves an otherwise equivalent current-directory component. The original native command control fails EROFS with that spelling; the unchanged fuzzer rejects it before host execution. Parent components remain unresolved so symlink-dependent permission targets keep their native meaning.
