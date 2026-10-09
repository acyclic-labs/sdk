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

Current source slice installs an executable-pinned diagnostics extension through
the existing Acyclic configuration ownership helper. Reinstallation is idempotent,
foreign settings are retained, and modified owned settings refuse replacement or
uninstall. Immutable extension assets remain cached after uninstall. No resource
starts during extension registration. The diagnostics command bounds process
time and output and invokes the selected binary without a shell.

This slice does **not** implement or qualify recursive agents. Pi continues owning
context, compaction and model retries. No Harness context/accounting parity or
process confinement claim follows from installation or diagnostics.

## Existing mechanism gaps on SDK 6aaed7e

- `pre_tool` prepares the existing durable spawn handshake but returns no exact
  prepared scope to a process adapter. Startup must consume that handshake after
  publication, without inferring authority from a mount path or FIFO position.
- `authorize_inspection` enforces ancestry. Destination-authorized sibling and
  descendant imports need explicit source read/destination write authority.
- Plugin adapter state has no explicit lineage-preserving scratch binding,
  durable Pi session handoff, child process ownership or message/wait contract.
  These need owning existing-mechanism seams before recursive tools are exposed.
- Native hook host validation excludes Pi. Adding its name alone cannot establish
  tool routing, scoped child identities or process admission.

Coordinate shared service changes before implementing these seams. Do not add an
extension-side authoritative registry, merge implementation or effect journal.

## Composition proposal against landed main

This is a source proposal; none of these additional paths is implemented or
qualified. Keep the Pi model loop outside Harness stock execution.

| Datum | Existing authoritative object | Pi adapter responsibility |
| --- | --- | --- |
| Session and agent identity | `AdapterState.root_session_id`, `Route.agent_id` | Persist the exact admitted `TaskId` binding alongside its route, with no second agent registry. |
| Task admission and execution owner | `CoordinatorTaskHost`, `TaskAdmissionRecord`, scheduler `WorkLease`/`LeaseFence` | Reopen the original admission; reject forged, stale or conflicting bindings. A stored fence is a reference requiring fresh verification. |
| Parent/child workspace publication | `PendingSpawn`, Filesystem workspace contexts and publication recovery | Bind exact parent, child and prepare operation; retain serialized preparation initially. Return prepared scope only after durable publication and state save. |
| Pinned Pi context | Pi `SessionManager` session file and selected leaf | Record the exact host-native parent session/leaf boundary. Use Pi's fork mechanism; qualify incomplete tool exchanges and later parent changes. No Harness context conversion. |
| Messages and consumption | Existing Stream mailbox through `CoordinatorTaskHost`/ordinary durable task commands | Publish immutable bounded `FileRef` payloads under verified sender/recipient grants; retain consumption in the existing task workflow checkpoint. No route-local mailbox. |
| Payload and private storage | `FilesystemHost`, `FilesystemSchedulerPayloadStore`, `ContentGrant` | Bind owner-held journal storage separately from child-visible volumes. Do not inherit root-private grants. |
| Process dispatch and uncertainty | Existing task effect journal and `NativeProcessProvider` launch/observation receipts | Prefer one-shot `pi --print --session ...` workers. Reuse exact approved process request and at-most-once dispatch; no Pi launch journal. |
| Process tree lifetime | `acyclic_native_runtime::ProcessTree` used by the native provider | Native provider owns launch/capture/drain. Adapter shutdown must not treat dropping a task reference as proof that process cleanup finished. |
| Cancellation and reopening | Existing coordinator intent, task checkpoints and effect reconciliation | Publish cancellation intent first; retain the original reservation through uncertain capture/cleanup. Reopen storage and validate admission/bindings before reconciliation; never auto-relaunch uncertain work. |
| Scratch and workspace imports | Existing Filesystem filtered fork/lineage and publication permits | Use real parent scratch lineage with empty selected content; source reads and destination writes require separate exact grants and pinned generations. |

`FilesystemTaskRuntime::open` already composes the Stream coordinator,
Filesystem payload store, content verifier, task host and worker journals. Prefer
that composition if it fits the Pi-only task machine; otherwise compose those
same public objects directly. Neither choice selects a Harness model/context
loop. Ordinary mail requires admitted durable tasks, not just a plugin route ID.

Main's native provider accepts no interactive stdin and captures a one-shot
process. Avoid expanding that shared provider for Pi RPC unless a demonstrated
acceptance requirement cannot be met by one-shot workers and extension message
delivery at safe Pi boundaries. Active steering, session reopening, provider
environment, session file publication and cleanup correspondence need actual
installed tests before this choice is accepted. An explicit later resume is a
new admitted process effect; it cannot retry an uncertain earlier launch.

Canonical Rust request/result contracts must generate the extension's consumed
types/schemas through repository generation mechanisms. No independently
maintained TypeScript authority schema. Pi host-native transcript access,
workspace reads/imports, messaging and lifecycle control remain separate grants.

### Native volume binding proposal (not implemented)

The plugin and `NativeVolumeView` currently name workspaces differently. The
native view opens the canonical `VolumeRef::storage_name()`; a plugin route's
mount path or workspace ID cannot substitute for that typed identity.

For new Pi sessions, evaluate creating the existing root and child workspaces
under canonical volume names from the outset. Persist the exact typed reference
on the existing root/prepared-child bindings, before creation can be retried.
The provider, owner, role, physical workspace ID and canonical name must agree
when reopening. Keep project and scratch roles separate from owner identity.
Do not rename existing workspaces, copy their contents, or add an alias registry.

The bounded source audit found these consumers that must change together:

- Initial root attach and later root adoption select the workspace name.
- Pending root registration recovery currently derives that name from session
  and physical path; it must instead recover the original typed binding.
- Child fork and pending-child discard currently derive names from fork/root
  identities. Both must resolve the same prepared typed child, including recovery
  before the fork has published a workspace ID.
- Normal root reopen already resolves the core context's workspace ID. Retain
  that identity check and verify its typed name rather than attaching a new root.
- `SharedRootRegistry` is keyed by canonical physical path and checks native
  identity/source identity. Preserve its shared watcher and epoch semantics.

`NativeVolumeView` prepares and owns its own working set below an empty native
directory. Pi preparation must have one native view owner; it cannot also mount
the same destination through the plugin's `LocalMount`. Retain the existing
Filesystem context, lineage, publication and cleanup authority throughout.
Existing Codex/Claude consumers retain their current paths. There is no legacy
Pi data to migrate. Shared host-binding changes require owning-seam review;
source inspection does not establish installed or recovery correctness.

Admit the typed task/agent identities before preparing their volumes, retaining
their exact association with the existing native route and context identities.
A fork key, agent identity and task identity have different roles; matching byte
representations do not establish an admitted association.

Scratch must not become a fabricated physical root or watcher registration.
Existing plugin lazy mounts require a physical source for each root; the landed
`FilesystemHost::fork_volume` already forks an exact source generation with
separate source-read/destination-write grants and `Some(vec![])` for an empty
selection. Use that path for scratch and retain its original creation generation
separately from the later mutable head. Retries resolve the original operation
generation. The prepared/route task binding can reference that volume without
adding it to the physical project-root index or converting Pi context into a
Harness conversation. Native view preparation and publication remain shared.

Pinned Pi source checks expose two important constraints: `agent_settled` is the
notification that excludes further automatic retry/compaction/continuation;
`agent_end` is insufficient. `SessionManager.createBranchedSession` replaces the
manager's current session, so it must never be invoked on the live parent manager
to create a child. `forkFrom` copies all source entries and does not itself select
the admitted leaf. Child preparation needs a separate manager over a verified
immutable parent cut, with the actual child cwd and service-owned session storage.
No implementation may substitute a raw current-file copy for that pinned cut.
Capture retains the native parent session ID, file location and cwd alongside
the exact branch boundary. Pre-admission revalidation rejects any changed source
binding or history. These are untrusted host references until the service checks
them against its owned session/route association; a file path grants no access.

Shared cancellation dependency: planning reports an I-owned bounded-model
counterexample where separate task cancellation and Filesystem lease finish do
not fence a later fresh permit. Pi must not assume cross-store atomicity or fix
this with an adapter ledger. Consume the owning qualified authority transition
that prevents both future permit creation/renewal and stale publication, while
preserving reconciliation of the original admitted effect. Recursive writer and
cancellation claims remain gated until that mechanism lands and is qualified.

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
