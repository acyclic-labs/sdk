# Pi integration qualification

Source work targets official `@earendil-works/pi-coding-agent` **1.1.0**, released as
[earendil-works/pi v1.1.0](https://github.com/earendil-works/pi/releases/tag/v1.1.0).
The immutable version's [extension declarations](https://github.com/earendil-works/pi/blob/v1.1.0/packages/coding-agent/src/core/extensions/types.ts)
and [configuration contract](https://github.com/earendil-works/pi/blob/v1.1.0/packages/coding-agent/docs/configuration.md)
are reference material. The host qualification lock pins the package integrity
to `sha512-SeEi/4hdcHNgA9UWlefZl7ZZpm3dzi2OoxNjDHsBJ9o298LNOtbL4DGKgitlEj6uCTccvtw6f2hlCkTPVJ2RXg==`.
The installed CLI JavaScript SHA-256 is
`e79626f2dd6f94aa45d30f3fa63cd84319a6eefcd150b353cfaf274366926774`.
The official repository's `v1.1.0` tag resolves to source commit
`abe508e1b89912adde45528136c3221eb69acdd7`; the read-only tag receipt is retained
in `target/pi-source-receipts/pi-tag.*`. Registry metadata provides no `gitHead`,
so the package integrity and official source reference are recorded separately.

## Installed control path

The existing configuration owner installs an immutable extension pinned to the
selected `acyclic` executable. Reinstall is idempotent; foreign settings and
modified owned settings are preserved. Registration starts no resources.

Pi's native `session_start` invokes `acyclic __pi SessionStart` directly, with
bounded stdin/output and no shell or extension retry. The existing service
catalog reserves the session before original-owner registration. Admission
persists the selected root command, task/agent/volume binding and `AdapterState`
before selected native allocation. Repeated startup reuses that admission;
changing the selected root for the same session is rejected.

`acyclic_agents` invokes `TaskStatus`, observing retained coordinator outcomes
without pulling a lease. `acyclic_cancel` invokes `TaskCancel`; only the original
root and authenticated retained routes are targets. Durable cancellation intent
precedes service shutdown. `/acyclic-doctor` remains a separate qualification
diagnostic. A missing acknowledgement is not permission to repeat an effect.

Pi owns model selection, native history, compaction and model retries. No Harness
context/accounting parity or process confinement follows from these controls.

### Native recursive tools

The production extension registers these tools through the authenticated owning
service; neither root nor child tool output includes caller proofs, runtime
environment, task leases or signers:

| Tool | Arguments | Native operation |
| --- | --- | --- |
| `acyclic_spawn` | `id`, `prompt`, optional workspace `source`, optional resource `root`, optional `context` (`scratch` by default, or explicit `native_cut`) | `SpawnPrepare`, actual SDK child session serialization, then `SpawnCommit` |
| `acyclic_send` | `id`, `message` | `TaskSend`, one original mailbox publication |
| `acyclic_steer` | `id`, `message` | `TaskSteer`, original native task steering |
| `acyclic_wait` | `id` | `TaskWait`, at most 120 passive nonblocking observations over 30 seconds; only an actual terminal outcome completes the wait |
| `acyclic_inspect` / `acyclic_diff` | `id`, optional resource `root`, optional `directory` (relative, directory root by default), optional `cursor`, optional `maximum_entries` (default 64, maximum 256) | `TaskInspect` / `TaskDiff`, authenticated pinned-source directory pages |
| `acyclic_import` | `source`, `destination`, optional resource `root`, `paths`, `confirm` | `TaskImport`, selected source reads and authorized destination writes |
| `acyclic_writeback` | `source`, optional resource `root`, `paths`, `confirm` | One `TaskWriteback` prepare; real native UI approval or outside-model host consent; headless `TaskWritebackStatus` passively observes the same original SDK operation |
| `acyclic_model` | optional paired `provider`, `model` | Actual Pi model registry lookup and `pi.setModel`; omitted arguments observe the native selected model |
| `acyclic_context` | none | Actual `ctx.getContextUsage` and selected SDK session/leaf identity, not Harness accounting |

Native context is fresh by default: a real independent SDK session serializes
only its actual header, with a new native identity and original caller provenance.
Explicit `context: "native_cut"` inherits only the caller's exact selected
pre-tool branch, excludes the entire spawning assistant/tool exchange, preserves
native compaction/context edits, and revalidates after asynchronous prepare before
commit. An incomplete projected exchange rejects admission.

`source` selects an independently authorized workspace, not message history.
Selecting another agent's workspace never reads, returns or clones that agent's
native transcript. Even explicit native context with a foreign workspace source
comes solely from the actual caller. The two cut variants (`caller_tool`, `scratch`)
carry their real selection semantics without fabricated ownership.
Fresh native context does not discard project contents: projects always inherit
the authorized selected workspace. Only native history and private scratch are
empty by default.

Resource `root` is an admitted RootID key, not a path or working-directory alias.
It may be omitted only when the actual selected source has exactly one project
root. Multiple roots require an explicit key; the extension never silently picks
the first namespace. Root selection is independent of native context selection,
and source-bound cursors remain tied to their actual selected resource.

Import merges into an already-admitted private task project using the original
selected-source READ and destination WRITE grants. Its `confirm` flag requests
the merge but grants neither permission; headless imports retain those same
authorization checks. Import leaves the source and physical root unchanged.
Publishing to the physical root is the separate approval-bound writeback below.

Writeback's `confirm` flag is a model request, not human approval. The first
control returns its immutable prepared approval digest. When Pi has a real
dialog-capable UI, the extension displays that exact digest, source, selected
root and paths through `ctx.ui.confirm`. That actual user decision invokes the
same pinned host-only `pi approve` CLI described below, never a publication
request authorized by a model/caller boolean. UI and headless modes then observe
the same SDK tool-call operation and prepared digest in one retained promise.
Decline, cancellation and uncertain acknowledgement
never trigger a replacement or automatic retry. Headless mode emits an immediate
SDK tool progress event tagged `acyclic.writeback.prepared`, carrying
only the original operation, source, root, selected paths and exact digest. Its
single retained callback performs at most 240 passive `TaskWritebackStatus`
observations over 60 seconds. Only the actual native `Applied` receipt returns
`published`; a decline is factual, and the observation bound returns `pending`
without inventing an operator response or publication acknowledgement.

The trusted host operator inspects `acyclic pi approvals --session <original-native-session>
--operation <original-tool-id> --json` and submits `acyclic pi approve --session
<original-native-session> --operation <original-tool-id> --digest <exact-prepared-digest>
--decision approve|decline`. The operator must independently inspect the original
root baseline, selected candidate and concurrent root changes before approving.
Inspection is metadata-only by default. `--project-files` requests a bounded
projection of the exact immutable imported generation; larger files use
`--file <selected-path> --offset <byte-offset> --maximum-bytes <1..262144>`.
The returned evidence cursor binds the original operation, root, generation,
path and offset. A partial page is not complete-file approval evidence.
The CLI obtains the original protected owner configuration at the host boundary;
no operator key, scope or bearer is given to the model or child. Neither model
`confirm`, an echoed digest nor a child caller proof supplies this authority.
This is a trusted-host boundary, not an OS sandbox against arbitrary custom code
that already possesses the protected owner configuration. There is no
model-visible approval tool or `approval_digest` argument.

Effectful controls retain the original SDK tool-call operation and its in-flight
or rejected result, including uncertain acknowledgement, for the extension's
lifetime (at most 4096 retained invocations). Repeating that identity with changed
arguments rejects; repeating it after lost acknowledgement never reseeds an SDK
session or repeats an effect. The service owns durable recovery. `acyclic_agents`
and `acyclic_wait` observe the retained original task. Native wait keeps the same
operation, task and SDK session across bounded passive observations, respects
the SDK abort signal, and exposes pending at its finite count/deadline rather
than inventing completion. It never repeats spawn, import or mailbox effects.
Explicit `confirm: true` is necessary for import/writeback, but does not substitute
for source, destination or root approval authority.

Root and child SDK sessions use the same native inbox consumer. `agent_start`
begins at most 120 passive `TaskReceive {after}` observations over 30 seconds;
`agent_end` and session shutdown abort outstanding reads. The service returns
an authenticated original item and decoded envelope, not a new receive effect.
Delivery uses Pi's actual custom-message steering/follow-up queue and retains
task, sequence, message identity and kind in its native session entry. Queue
acceptance is not acknowledgement: the next cursor advances only when that
own-task entry is present in both the selected SDK branch and actual persisted
native bytes. Inherited parent-task cursors are ignored. Lost acknowledgement
stops automatic inbox polling; no service ACK API, receive ledger or competing
IPC delivery engine is introduced. The service receives no foreign native
transcript locator; explicit native context is captured by the caller's SDK.

`node --experimental-strip-types --test pi-context-mail.test.ts` in
`plugin/tests/hosts`, with the final absolute `ACYCLIC_BINARY`, exercises actual
installed root/child follow-up and steering delivery through the production
extension and Pi's asynchronous deterministic provider. It is unrun source,
not qualification evidence.

`node --experimental-strip-types --test pi-context-fresh.test.ts` exercises actual
first-provider-turn root-only exclusion and child-only instruction preservation.
It also verifies that a foreign workspace's files remain usable while its private
native transcript is absent; explicit native context inherits only the caller.
The probes inspect the complete provider context without filtering role markers.

The outside-model `pi-operator-driver.ts` consumes actual JSON tool updates or an
explicit original SDK operation. Before deciding through the pinned CLI, it
checks independently supplied initial-root and selected-content oracles, actual
staged/root files, immutable imported SDK bytes bound to the original selected
source seal and import receipt, native preimages,
generation identities, and BLAKE3 of the exact bounded canonical preparation
bytes. Rejection controls
also require the original prepared ticket and root contents to remain unchanged.

These tool registrations and focused fresh-context/workspace/uncertainty tests
are source changes, not exercised qualification evidence. Parent integration
must run `npm run test:pi-context` in `plugin/tests/hosts`, then the installed
recursive native fixture against the final source-built binary. Earlier receipt
hashes below describe their recorded source revision, not this edited extension.


## Durable authority and resource state

There is one service catalog and one `AdapterState`/route graph. `PiTaskRuntime`
uses the existing Filesystem task runtime, coordinator, effect history and
checkpoint machinery. Protected original owner credentials are loaded before
Stream history; reopening never creates replacement credentials. Private journal
write authority is not inherited by tasks; original allocation inputs are
published in their dedicated read-only directory.

Root and scratch allocation use required `Preparing`/`Created` discriminators.
Preparing retains exact intent and identities, not usable resource authority.
Created requires real immutable workspace/source/native identities and the
original creation generation; a source epoch of zero is valid. Paths, current
workspace heads and adapter flags are not allocation or process-success evidence.
An uncertain original allocation cannot be replaced with a fresh one.

`pi_root_provider.rs` implements that allocation as the first-party at-most-once
effect: lazy native project attachments use the original volume storage names,
and scratch uses a real portable durable workspace's original creation generation.
Its original fresh task owner first commits the real `BindConversation` action
under the same protected key, task fence and Conversation tail guard. This does
not mirror Pi-native model history or invent project/resource publication events.
One immutable attachment input pins all selected source/native identities before
any project creation. Canonical upload metadata and original Filesystem creation
operations, not stored paths or mutable heads, authenticate the immutable result.
An uncancelled original owner may finish its original bounded local creator after
a lost acknowledgement. Cancelled and historical observers never create missing
resources; reopening a created project additionally requires its original sealed
READ authority and immutable creation evidence. These sources are not qualification
evidence.
The same bounded canonical publisher stages control, mailbox and child-intent
inputs under the original operation's `pi-inputs` directory. Tasks receive read
authority there, not the protected owner's private journal writer.
Signed operator scopes instead use root-only `pi-operator-inputs` under the same
original publisher. Recursive children inherit no read grant for that directory;
ordinary control inputs carry its immutable file reference, never the bearer.
Root stock command outputs bind to the original unmounted root result scratch
after canonical successful-creation and original binding-ACK verification. The
SDK verifies the admitted task's exact write grant and original FS operation,
then installs its opaque output source in the original context's fixed slot.
No new scope, context clone or bootstrap-journal writer is exposed.
The configured `SystemUnixMillisClock` is zero-sized; its opaque Arc control
header is outside the logical payload model. Native bootstrap borrows that
trusted configuration, without inventing zero-work allocations or a new clock
policy. A custom clock's owner remains responsible for its actual payload.
The finite Root driver recovers the original reservation before discovery and
keeps its lease/attempt custody through the worker result. Checked native command
futures retain the original shared context instead of cloning or retrofitting
ordinary contexts. Root output binding precedes every scoped control adapter.
Recovered lease and allocation custody enter the native worker together, before
cancellation checks; early denial retains the original unresolved reservation.
Caller scopes use the protected coordinator's original issuer on that same
ledger. Their exact claims are compared against borrowed retained bindings,
without copying a second claims DTO or giving callers operator or file grants.
Root allocation observations return their actual output reference, allocation
DTO and original allocation custody together. Warm and cold service consumers
must retain that custody through source restoration and activation; recorded native
paths alone never prove creation.
Opaque original scratch-allocation evidence is separate from native disposal
authority. Releasing SDK-owned scratch additionally requires its original owned
preparation, current fenced write grant and drained effects; never project or
user-source roots.
Selected root-source authentication consumes the caller's original native work
and sealed read context plus the same successful effect, uploads, FS creation and
native identity; it neither grants authority nor exactifies the lazy tree.
Approval digests use the existing canonical BLAKE3 identity through the shared bounded codec, including
actual hash bytes and owned-buffer custody.
Native command metadata uses the shared encoder's actual guarded JSON Value on
the original native work/cancellation ledger, retaining logical payload and
container-slot custody through its final runtime consumer. Audited parser quotas
are conservative admission, not invented physical allocation or OS-memory work.
Only SDK-observed String/Vec allocator attempts increment allocation operations;
logical retention and opaque parser-quota adoption do not invent those attempts.
Native bootstrap/mail input staging and readonly original-operation lookup use
the same protected publisher key as their measured counterparts, not an alternate
once-file namespace. Closed typed decoders reserve their audited logical DTO
ownership plus parser scratch before parsing; raw input remains guarded through
decode, and decoded ownership remains guarded through the DTO's final consumer.
Mailbox decoding is restricted to its audited value-free envelope and stock
command types. Fixed mailbox denials retain static wire codes without allocating
diagnostic strings after cancellation or quota exhaustion.
Malformed persisted DTOs retain the SDK's original typed serde diagnostic and
its admitted parser quota until the native error consumer drops it; empty
codec-error guards are not manufactured. Native canonical-byte and digest
helpers use that same original ledger and the existing SDK canonical/BLAKE3
identity, rather than rebuilding a remaining-budget meter.
Native typed decoders declare both their concrete Value-field schema and a
finite inventory of actual serde fields, variants and static expected labels.
The shared SDK source pass issues the shape used to admit that diagnostic
custody before parsing; no arbitrary label allowance or second parser is used.
The Root provider's dispatch and reconciliation enter through funded SDK futures
on that same request ledger. A cancelled original creator can observe and publish
complete original resource evidence, but cannot capture or create missing resources.
Closed Root artifact decoders use concrete envelope sizes plus SDK-issued source
shapes rather than application bytes-to-heap multipliers.
Root allocation schemas serialize borrowed, fixed wire descriptors through this
same bounded encoder; the native path does not first build an unguarded JSON
schema. Binding validation compares retained full volume identities and borrowed
root keys without allocating a second identity index.
The native conversation binder consumes the protected issuer's original
authority, scope and metadata custody without cloning the scope. It resolves
the original binding event through the SDK's authenticated atomic operation
index, including archived events, then replays the same prior revision and
guarded command for its original ACK. Cold observation never installs that ACK
in a current owner's binding cache.
Child cancellation and preparation rollback use the existing guarded SDK task
cancellation engine on their original cleanup work. The native process stop
receipt remains separate: a cancellation acknowledgement alone is not proof
that a process drained or that scratch can be disposed.

Imported-file evidence retains its original SDK allocation guard through the
existing service actor, response channel and bounded RPC encoder. Encoding
admits its separate output buffer on that same ledger before growth; the
transport retains its custody until the actual response write completes.
Legacy replies carry no native allocation claim. These are SDK-owned-buffer
bounds, not a measurement of Pi, Node, transport-library or total OS memory.
Live selected-source lookup verifies the original owner on the caller's native
work before copying prepared binding metadata. Its returned live-source object
retains that metadata custody through fork, selected import and physical cleanup.

The pinned Pi `__pi` entry point uses that native path end to end: bounded
stdin and caller-scope decoding, original native current-directory/environment
metadata, and prepaid hook-name/request-ID storage. Its one query ledger
survives the existing at-most-once transport and final guarded stdout encoding.
Only an unavailable service permits bootstrap retransmission of the same
envelope; an uncertain acknowledgement is not automatically resent. Typed
native failures remain owned through the final CLI diagnostic, and a failed
service close exits unsuccessfully without boxing or replacing its native cause.

Shared physical roots retain the existing single-flight registry, watcher and
lazy source. Acquisition requires an explicit finite work budget/cancellation
token; failed nested acquisition preserves spent counters. These counters bound
logical backend requests, not all OS or notification-library allocations.

Native context helpers capture the selected leaf and immutable native branch,
revalidate before admission and create a separate child manager. They do not
mutate the live parent's manager or replace a native branch with a raw file copy.
Native `agent_settled`, rather than `agent_end`, is the stable host completion
boundary after automatic continuations.

## Source-bound controls and qualification

No checks for this source change have been run. Parent integration must execute
these commands against the resulting source-built binary:

```sh
cargo build -p acyclic-plugin --bin acyclic
cargo test -p acyclic-plugin --bin acyclic
cargo clippy -p acyclic-plugin --all-targets -- -D warnings
```

In `plugin/tests/hosts`, with absolute `ACYCLIC_BINARY` selecting that binary:

```sh
npm ci --ignore-scripts --no-audit --no-fund
npm run test:pi-context
npm run test:pi-bootstrap
npm run test:pi-session
npm run test:pi-installation
npm run test:pi-service
```

The service case loads the installed extension through pinned Pi 1.1.0, uses
native model/tool execution, switches models, kills/restarts the owned service
and cancels the retained original task. It verifies original identity and native
context preservation. It deliberately does not count a Preparing root as child
execution or provider allocation.

The Rust bounded explorer replays actual Pi machine checkpoints under a fresh
registry, including malformed/foreign/repeated results, cancellation and
indeterminate outcomes. Negative controls remove reference preservation or
repeat the allocation command. Its bounds do not establish storage/process or
platform correctness.

Recursive launch, mail/wait/steering, scoped destination imports and approved
root writeback require the authenticated created-resource admission producer,
bounded immutable output staging, the real native process provider and consuming
workflow. Their installed acceptance gates below remain open; diagnostic/task
controls and machine exploration do not qualify those behaviors.

## Open qualification gates

| Invariant | Production mechanism / assumption | Required verification | Current evidence |
| --- | --- | --- | --- |
| Owned installation preserves foreign configuration | Existing configuration ownership and immutable extension assets | Install/reinstall/upgrade/uninstall, modified settings and corrupted assets | Focused Windows ownership and actual public installation/doctor lifecycle passed; cross-platform host workflow remains required |
| Child history is exactly the admitted native cut | Synchronous capture, pre-admission revalidation and a separate Pi manager; pinned Pi 1.1.0 | Native branch, compaction, context edits, complete exchanges, empty cut, bounds and parent-change cases | Eleven native SDK/loader tests passed on Windows; service admission still pending |
| Recursive identity and dispatch remain authoritative | Existing prepared spawn, admitted task/agent binding and original native effect | Installed root, two children and grandchild; exact retries; forged/stale scope rejection | Integration pending |
| Scratch starts empty while retaining lineage | Existing pinned selected fork and retained creation generation | Nested empty scratch, retry after child edits and reopen | Pi binding pending |
| Tools operate within admitted destinations | Existing route/tool classification and exact content/publication grants | Installed read/edit/write/shell, escape and unknown-tool denial | Pi routing pending |
| Messages and steering survive restart | Existing ordinary task mailbox and workflow checkpoints; safe Pi boundaries | Root/child/grandchild delivery, duplicate publication, wait, steering and consumption acknowledgement | Integration pending |
| Inspection remains bounded and authorized | Existing paged task metadata and pinned Filesystem reads | Ancestry/sibling/descendant access, pagination invalidation, fixed active work with increasing retained data | Pi contracts pending |
| Imports change only caller-owned destinations | Existing source-read/destination-write grants and pinned import plans | Sibling/descendant imports, source immutability, conflict handling and rejected unauthorized writes | Pi contracts pending |
| Root writeback matches exact approval | Existing approved publication and user-edit reconciliation | Approved/rejected/stale plans and concurrent user edits | Pi flow pending |
| Reopen never repeats an uncertain effect | Existing task/effect receipts, publication recovery and process-tree owner | Close/reopen; faults at admission, launch, acknowledgement, observation, publication and cleanup | Pi integration pending |
| Cancellation fences writers without losing reconciliation | Owning qualified task/Filesystem authority transition | Late fresh permits, stale owners, cancellation races and bounded transition checks with negative controls | Shared dependency and Pi qualification pending |
| Installed behavior matches each supported host | Exact package/tool hashes and public installed entrypoints | Windows, Linux, WSL, macOS and applicable PTY runs; affected Codex/Claude regressions | All Pi platform runs pending |

## Historical receipts

The following retained observations bind older source/tool hashes only. They are
not current service/recursive qualification and must not be reused as passing
evidence for the task-control changes above.

The first Windows installer test build reached its 15-minute deadline before
producing a test result. Its owned process tree was terminated; exact inputs,
tool hashes, raw logs and closure are retained in
`target/pi-source-receipts/pi-installer-native.*`. This is no passing evidence.
A later focused ownership test passed on Windows in an isolated unoptimized
target, including repeated installation, foreign settings, modified-owned
settings refusal, uninstall restoration and corrupt-asset refusal. Its receipt
is `target/pi-source-receipts/pi-installer-unit.*`.
Required
installed root/two-child/grandchild, pinned context, scratch lineage, scoped tools,
steering, destination merges/root approval, restart, fault injection, bounded
models and Windows/Linux/WSL/macOS PTY gates remain open. Existing Codex/Claude
regressions are required for shared changes. Missing or skipped gates cannot pass.

Local dependency locking, installation, builds and tests are authorized. Keep
owned caches/artifacts isolated, coordinate actual host contention and retain
exact source/tool/features/OS evidence and owned-process cleanup. The Pi owner
owns its focused main-based PR, required CI, protected merge and independent
origin/main verification. Shared dependency questions go through planning;
protection bypasses and unrelated source imports are excluded.

Native context test sources live in the existing `plugin/tests/hosts` package.
Run `npm ci --ignore-scripts --no-audit --no-fund` and `npm run test:pi-context`
there with the pinned host inputs. The test imports the
real Pi SDK and an exact temporary copy of the extension below that package;
this avoids custom module resolution and does not test extension discovery.
An additional case calls Pi's native extension loader on the original source
outside the host package, with isolated project/agent directories, and checks
registration without starting diagnostics. It is not an installed CLI test.
The eleven native tests passed with no skips on Windows/Node 24.4.1 on
2026-10-09. The receipt binds extension SHA-256
`0766bf4299e12856ca73abdc07eecbfe591bfab7a997a950d08c117c5246f32e`,
test SHA-256 `311686eff1cbe9ffdba856d8e362559f8a967ab4b2db970c7951f6598225834c`
and host-lock SHA-256 `c181912555a7a0d8a0666b3208e67b7d4256c81b041e9b1d5beba1887589d20f`.
Raw logs and tool hashes are retained in
`target/pi-source-receipts/pi-context-native.*`. This evidence covers the native
context helpers and loader registration; recursive service and installed CLI
qualification remain open.

The supported adapter-state contract is solely v1. Current producers write a
required tagged task binding on the root, pending spawn and route records.
Existing host-managed consumers select that explicit variant; Pi selects typed
task/agent and volume references. Missing historical binding fields and stored
v4/v5 states are rejected. No migration, alias, default reader fallback or
old-state relabelling is provided. Pi references require semantic and owning
runtime verification before activation; their presence grants no authority.

Retained Pi references are checked before workspace reopen for bounded native
session identity, exact root selection keys, volume roles and provider, private
scratch ownership, original creation shape, and duplicate task/agent/session or
project-volume identities. Mixed Pi/host-managed records are rejected. These
checks do not authenticate an admitted task or establish that a workspace ID
belongs to the stored volume. Exact owning-store correspondence, retained task
lease/grants and native-view preparation remain required before activation.

The current carrier and rejection test sources passed
`cargo check -p acyclic-plugin --bin acyclic --tests --locked --jobs 2` on
Windows with Rust 1.98.1. Source hashes, raw logs, exact tool hashes and closure
are retained in `target/pi-source-receipts/pi-binding-check.*`. This checks
compilation, not execution or recursive behavior. The local lock delta adds
only the plugin's Harness and Stream dependency edges. The separately owned
shared schema resolution must be preserved when the manifest graphs are joined.

Three retained-binding rejection tests and the exact explicit-v1 state test
passed on Windows with no failures or skips. Their source hashes, tools and raw
results are retained as `pi-binding-tests.*` and `pi-state-v1-unit.*` in the same
receipt directory. These and the passing installer test used isolated
`target/pi-rust-unit` with dev-profile overrides `opt-level=0`, `debug=0`,
`codegen-units=8` and two Cargo jobs. They provide no performance evidence.

Scratch allocation is a required tagged preparing/created variant. Missing,
null, unknown and incomplete allocation fields are rejected; preparing never
implies a new effect may replace an uncertain original allocation.

The reviewed explicit-allocation successor passes strict Clippy with
`--tests --locked -- -Dwarnings` and the complete Windows plugin unit suite:
109 passed, zero failed, two explicitly ignored. The native Rust compiler
mount case was then invoked explicitly and passed without skips; the remaining
ignored entry is the subprocess writer helper exercised by the mount lifecycle
test. These are existing plugin/native regressions, not recursive Pi evidence.
Receipts are `pi-maintainability-clippy-r2.*`, `pi-plugin-unit-suite.*` and
`pi-native-compiler-regression.*`. The tested Pi Rust source SHA-256 is
`77a6283f7d7919b768c6c5934ac2a1b2a90caee3e8e697633f5b9f55ae284718`.

The actual public CLI installation lifecycle also passed on Windows without
skips. Build the `acyclic` binary, set `ACYCLIC_BINARY` to its absolute path and
run `npm run test:pi-installation` in `plugin/tests/hosts`. This test isolates
user, project and data directories; installs both scopes; loads each pinned
asset through Pi's native loader; verifies repeated installation, foreign
settings and refusal to overwrite user edits; and restores both configurations
through public uninstall. Pinned assets remain available after uninstall.
Receipts are `pi-installed-binary-build.*` and `pi-public-installation.*` under
`target/pi-source-receipts`. The tested binary SHA-256 is
`76ec4041969c611e05f247c8afbafef8c8ffeb6b41de9d7cf06234b2a76536da`,
and installation test SHA-256 is
`d651c4c6c332da6b8a73715c77999c6f565efe9275912d6e0181eb5c1ef02ab4`.
This covers installation and loader registration; recursive execution remains
unqualified.

Native context qualification also enumerates 48 branch histories: one to four
user entries, zero to two excluded alternative entries, with/without a complete
tool exchange and with/without native compaction. Each case compares the child's
projection to Pi's own pre-spawn projection, checks the exact selected leaf and
unchanged parent, and verifies that a later parent mutation cannot change the
pinned child bytes. This is bounded execution against the production helper and
native SDK, assuming a synchronous session snapshot and service-validated
child identity/destination. It does not prove arbitrary histories, admission,
runtime recovery, grants or process effects. The context-test source SHA-256 is
`4cdd47caa8fea3418c896e2c1a892a02737f2a92c9f1605dc1c6abc222c4d82e`.

The refreshed CLI and all fifteen native host tests passed without failures or
skips in `pi-native-final-r4.*`. The public installation test additionally
executes doctor through the real isolated service: installed settings report
pending qualification, user-modified settings fail ownership, and removed
settings report no installation. Cleanup drains the fixture's service and
verifies its endpoint is absent and lock acquirable before deleting state.
The refreshed binary SHA-256 is
`471acf0f00eb6e9e75c950bfb0eb92ad60683888e375c1c220456f96b036de17`;
installation-test SHA-256 is
`890169ab46c933848e58c0569cbc55c8e44972eed74ca741c0a9bdf13ef95961`.
The earlier `pi-native-final-r3.*` failure records a test assertion using
`message` instead of doctor's `detail`; it is retained rather than counted as
passing evidence. Diagnostics still claim no process confinement or recursive
qualification.

Doctor verifies that owned settings select the current executable's pinned
extension and checks its exact bytes with a bounded read. Missing or corrupted
assets fail with their path. The installer and doctor share this integrity
check. The installed lifecycle test reproduced the previous binary's incorrect
warning for corruption, then passed with the rebuilt binary, including both
corrupted and deleted assets. All fifteen native tests passed without skips;
receipts are `pi-doctor-review-negative-control.*` and
`pi-doctor-review-native.*`. Strict Clippy passed in
`pi-doctor-review-clippy.*`.

## Native recursive runner contract

`pi_native.rs` consumes the existing `NativeProcessProvider`, task-owned effect
plan/approval, and canonical created-child content binding. Its mixed view
uses the existing lazy project mount adapter and original scratch
`NativeVolumeView`. The authenticated factory performs bounded owned-directory
creation, original preparation-journal recovery and actual SDK `mount_measured`;
it adds no project materializer, physical-root watcher, journal or process runner.
Per-volume lazy snapshots remain explicit in approval. Native processes are
trusted effects, not confinement. Native children have a separately created,
unmounted private result volume; results are never stored inside a native view.

The original root SDK supplies Pi 1.1.0, an absolute executable/CLI and installed
extension, and the complete cleared environment once. These immutable host
settings stay in the protected Pi owner file. Reserved recursive credentials
are removed before retention; each child receives its separately authenticated
original task/route credential. A native session ID or cwd is not authority.
The required child alias is retained in the original immutable launch input,
matches `[A-Za-z0-9_.-]{1,128}`, and remains separate from task/agent/native IDs;
recovery checks the original input rather than trusting a mutable alias catalog.
Default child context is a fresh native version-3 header with a separate child
ID. Explicit `native_cut` preserves only the caller's pre-tool branch,
compaction/context edits and captured timestamp; a foreign workspace source
never supplies the child's transcript.
The native request explicitly selects read/write/edit or no built-ins; ambient
extension discovery and MCP remain disabled.
The installed model-facing read/write/edit definitions use the existing Pi
tool-call seam and authenticated SDK file operations, not stock unrestricted
Node filesystem access. Original caller/task authority and declared native
volume proofs precede held-path IO. Children may access their admitted project
and scratch; the unmounted result volume is not a native path. The external
Root session reads its authenticated original source; writing the physical
user root requires the separate exact-digest operator publication.
Outside-host absolute/parent paths, foreign drives or shares and escaping
symlinks are denied before IO. Read payloads are capped at 262144 bytes before
stock text/image projection; exact edits retain the original read digest for
conditional SDK writes. Other ambient filesystem/command tools are denied.
This model-tool boundary does not confine arbitrary approved native programs.

The exact prompt is bounded native stdin, not a positional CLI argument: leading
`@` and option-looking text cannot become file expansion or change runtime flags.
The immutable native policy allows three minutes, five seconds per control check
and a 250 ms poll interval. Result capture stays below the original root file and
payload bounds and 1 MiB; raw output also leaves the SDK's worst-case JSON and
metadata allowance. Spawn arguments cannot widen these limits.

Launch must run outside the session/catalog actor so recursive child RPCs can
use it. Lost acknowledgement/restart uses original effect reconciliation only.
Both effect operations borrow the provider's original funded plan and use the
runner's original ledger. Their returned `(EffectStatus, NativeAllocation)` stays
intact through status comparison and completion encoding; restart does not clone
the plan or erase the returned status custody.
Cancellation orders the existing task/subtree intent; only the real provider's
observation establishes process cleanup. Every destination publication uses the
original fresh task guard within its actual Filesystem publication boundary;
already completed destinations remain committed on a later failure.

False-close signals every original warm provider before joining any launch
future, then authenticates its process-tree/stdio drainage without cancelling
the task or mounted view. Cold recovery cannot recreate that supervisor.
Bounded receipt queries use the original journal and actual history/codec
ledger; original capture/publication work is never charged again. An authentic
terminal outcome allows read-only cleanup observation, not a new publication
or deletion lease.

The lazy mount guard receives the SDK's actual shared native invocation carrier.
Original owner/grant IO, native callbacks and publication retain that same
allocation/cancellation ledger; no remaining-budget meter or receipt is rebuilt.
Preparation and view factories use the shared `NativeFutureBundle`; callers
check its constructor before awaiting (`...? .await?`). The original ledger
admits the compiled future frame before its builder runs, without a Pi wrapper
or second ledger. SDK mount, scratch-view and provider payload guards reside
inside the actual Arc-backed values, so every clone retains their custody.
Logical SDK byte accounting excludes opaque Arc/allocator headers and process RSS.
The protected conversation scope and verifier are funded before construction;
their owned tuple transfers into the live mixed-view project binding with its
original custody guard, rather than an external host cache. The separate
filesystem mount source never derives task authority from that accounting guard.
The current SDK-issued `NativeTaskContext` creates one opaque
`NativeVolumeCallbackGuard` for each exact selected binding; preparation checks
the ordered guard slice, and project mounting obtains its source Arc only through
the guard's original owner/volume/fence verification. Pi does not implement a
second callback-authority engine or convert a cold read context into dispatch.
Task-backed scratch and project sources install their actual retained task
owner, current fence and exact volume READ/WRITE purpose before preparation,
source capture or watch startup. Every callback verifies that authority on the
same original ledger. Its typed denial slot is funded before verification and
retains the whole original error and custody; no formatted failure or global
error trap replaces the cause.
The real driver callback scope is acquired under its short session mutex; the
mutex is released before held-file IO awaits. The actual native process retains
each project's owned original-work scope until its tree and stdio are physically
drained. Each accepted built-in IO payload owns a zero-heap scope fork through
its own physical completion; dropping a caller future does not drain queued IO.
These scopes account namespace activity, not client identity or OS confinement.
Original post-mount witnesses, not fresh
cwd observations, authenticate project built-in IO. Request/schema/plan uploads,
observed effect results and copied mixed-view metadata keep their actual SDK
payload guards through their final consumer. Intent loading, original runtime
binding and request construction use borrowed DTOs and the original bounded
native codec before owned copies. A partial startup retains the actual native
session, including its source watcher and destination guard, until bounded stop
and `cleanup_state().cleanup_complete` establish physical drainage.
Native cut decoding declares its exact Value schema; closed DTOs declare no Value
fields. Their audited fixed metadata accompanies the canonical SDK's actual
source-shape admission, rather than an input-byte multiplier. Process result
controls use the SDK's own closed-record decoder and retained-capacity receipt.
Closed launch, header and cleanup records supply a finite source-derived serde
diagnostic label inventory before parsing. Arbitrary Value cuts have no custom
labels; SDK request/result decoders own their respective diagnostic contracts.
Protected host-runtime copies use direct funded String, path and prefix-vector
operations, not a codec round-trip. Environment pairs acquire logical metadata
quota before insertion; opaque standard-library tree nodes are excluded rather
than reported as invented allocator attempts.
Runner construction borrows the staged plan; the original SDK provider owns its
funded plan graph, rather than a second unguarded runner copy.
Publication permits come only from the original task's actual SDK reservation.
Normal cleanup confirms the authenticated native process result and stops the
actual project drivers, retaining SDK session scratch. Explicit scratch disposal
requires the original opaque created-child Scratch proof, original native
root/leaf/preparation identities and current-fenced WRITE on every deletion.
Unknown pre-journal paths or replaced directories are preserved as uncertainty.
Original preparation rollback is not post-execution session cleanup.
Pinned process fixtures preserve the whole canonical reservation failure. Their
physical root is marked pending before reservation IO and is deleted only after
an actual successful gate release; uncertain setup, failed cleanup or an
interrupted test retains and reports the root rather than deleting beneath a
live cleanup owner.

Required source-bound lanes (not executed by this change):

- `npm run test:pi-builtin-paths` in `plugin/tests/hosts`, with the same final
  absolute `ACYCLIC_BINARY` and pinned native artifacts. Actual model tool calls
  must deny harmless outside-host read/write/edit markers, parent/drive/share
  escapes and escaping symlinks while permitted project/scratch reads, writes
  and exact edits succeed. This also covers genuine unknown-tool denial,
  bounded payload failure and original process cancellation; it never probes
  signing keys or substitutes a mocked filesystem host.
- `node --experimental-strip-types --test pi-native-recursive.test.ts` in
  `plugin/tests/hosts`, with `ACYCLIC_BINARY` selecting the final built binary
  and the locked Pi package installed. The fixture installs the production
  extension, supplies its deterministic provider through the ordinary original
  root SDK bootstrap, and requires root/two-child/grandchild and foreign-workspace
  execution, independently fresh first-turn context/PID oracles and caller cuts,
  isolated native read/write/exact edit, message/steering/wait, bounded
  inspection/diff, destination imports/root writeback, independent outside-model
  host inspection/consent with a wrong-digest engine denial, forged-source/
  destination/caller denials, and service reopen/model switch without repetition.
- Rust `required_pinned_pi_native_*` tests require absolute
  `ACYCLIC_PI_TEST_NODE`, `ACYCLIC_PI_TEST_CLI` (the package's
  `dist/bundle/cli.js`) and `ACYCLIC_PI_TEST_PROVIDER_DIR` (an owned directory
  below the pinned host package). They use real local Filesystem/Stream,
  approved native effects, caller-ACK loss, disk runtime/provider reopen,
  observed cancellation cleanup, and warm supervisor drainage without task/view
  cancellation. They are explicitly ignored in ordinary
  workspace checks because they require the privileged pinned-native lane; run
  `cargo test -p acyclic-plugin --bin acyclic required_pinned_pi_native -- --ignored`.
  Within that required lane, missing pinned artifacts fail rather than skip.

These new lanes carry no qualification claim until the final integrated source
and required native platform/provider lanes are exercised with raw evidence.
