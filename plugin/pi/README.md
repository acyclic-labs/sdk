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
