# bb2 tool installation

All lanes and the efficiency loop must install bb2 tools through:

```
sudo /usr/bin/python3 -B /mnt/bb/<candidate>/scripts/buildbox/bb-install.py --source /mnt/bb/<candidate>/scripts/buildbox
# prints /mnt/bb/tmp/bb-install-<qualified-generation>
# after acquiring the idle PR mkdir lock:
sudo /usr/bin/python3 -B /mnt/bb/tmp/bb-install-<qualified-generation>/bb-install.py --publish /mnt/bb/tmp/bb-install-<qualified-generation>
```

Use the reviewed candidate's installer for preparation, then its frozen copy
for publication. The candidate owns both its tool list and mandatory suites.
An older installed installer cannot qualify newly added tools. Preparation
checks that its own bytes match the source installer, and never copies an
earlier generation's qualification token into the new stage.

Publication is whole-generation only; there is no per-tool selection. The
qualified tool set is written to `/opt/buildbox/generations/<id>` and becomes
visible by one rename. `/opt/buildbox/bin` (pinned at `/home/var/bin`) holds
only absolute links through `/opt/buildbox/current`, so every lookup resolves
within one complete generation, and a tool absent from it is absent rather
than left over from a predecessor. `/opt/buildbox/previous` keeps the
predecessor intact on disk. With admission drained (upgrade lock held, daemon
retired), publication:

1. canaries the staged generation: a real undeclared `bash -c` job and a
   real `cargo build`, through `build-slot` with the stage bound over the
   client entry and a private admission root (cold cache, since the host
   cache authority is not yet switched);
2. switches host services and `current` to the new generation;
3. repeats both canaries through the live entry, and on failure switches
   `current` and its services back to the predecessor before re-raising.

Lane jobs therefore never observe a failing or mixed generation. Files the
installer does not manage (other owners' shims and binaries such as
`bb-cargo/cargo` or `tofu`) carry forward byte-exactly into the new
generation; tools managed by this or the predecessor's installer never do.
Roll back by re-activating a retained generation through the same canaries:

```
sudo /usr/bin/python3 -B /home/var/bin/bb-install.py --activate /opt/buildbox/generations/<id>
```
 Qualification
records installed predecessors and publication refuses if any tool changed
since. The Cargo source-namespace controls are mandatory in the installer's
qualification suite. The first publication onto a regular-file store adopts
those files as a retained `legacy-*` generation.

The installer freezes a root-owned candidate, runs the receipt, runtime,
admission and publication controls as `var`, and publishes only after they
pass. Each generation file is written beside its siblings, flushed and
renamed, and the generation directory itself is exposed by one rename.
Failed qualification or a failed canary changes no live tool.
`installed-tools.json` records the qualified hashes. Bin directories and tools
are root-owned and unwritable by the job/lane account; direct writes fail.
The canonical store is `/opt/buildbox/bin`, below protected root parents.
`home-var-bin.mount` pins the legacy `/home/var/bin` entry to that store, so a
lane cannot rename it through its writable HOME parent. The entry is read-only;
publication writes through the protected canonical store. The mount is enabled
at boot. The installer verifies that the entry and store have the same inode.
Do not use sudo to bypass this boundary. The installer is the only privileged
publication path. Existing jobs keep their running interpreter/inode.

Take the coordinator's idle `Q:/lanes/pr-gate/.lock` mkdir lock for the final
publication, releasing it immediately afterward. Qualification can happen
before this lock is acquired. Never interrupt a running gate. Daemon upgrades
must drain existing leases before stopping the idle daemon; replacing its file
does not replace already loaded code.

The gate drain owns a separate SQLite lifetime lease through `gate-queue.py
STATE dispatch DRAIN_SCRIPT`. Queue producers use the short queue transaction
and do not wait for a build. Each prepared run exclusively creates its own pin;
`gate-merge.py record` and `merge` both require that pathname with `--pin`.
Never reconstruct a pass from the mutable shared checkout or another run's pin.
A fence mismatch keeps its candidates queued and invokes the single checked
preparation repair while holding the gate lock. A retained infrastructure
record is diagnostic, not permission to refuse unrelated healthy work.

The current tool list is the SDK-owned runtime set. Another tool must be added
to the installer's explicit list with its required checks before installation;
an unreviewed filename cannot silently become a privileged tool.

Cargo's read-only metadata/tree/locate-project/pkgid/verify-project/read-manifest
commands bypass build tickets centrally in bb-admit. They retain the same
receipt and job HOME boundary. Callers need no tenant-specific copy of this
policy. Wrapper executables (including script interpreters) are resolved at
admission; an unavailable wrapper returns typed admission-INFRA (224).

Admission records every caller resource limit (both soft and hard) before
crossing the daemon boundary. The scope launcher restores that exact set as
root before dropping to `var`, without a PAM session resetting it again. The
HOME namespace repeats the restoration before its final privilege drop.
Public shell entries capture before starting the Node receipt wrapper: libuv
otherwise raises soft nofile to hard nofile before admission observes it. A
nested entry captures its own current limits, preserving deliberate changes.
There is no fixed nofile limit. Terminal runtime receipts record both the
requested and execution limits; mismatches or malformed limits fail as INFRA.

Cargo storage admission precedes every cache bypass, including cold builds.
Cargo's own metadata resolves the effective target and intermediate build
directories from CLI overrides, environment and configuration. Both must be
on the data filesystem and inside the current job or its receipt-proven private
target mount. The launcher and Cargo proxy share the same filesystem check.
The cache consumes this admitted target; it has no second path selector.

Use expanded Cargo commands instead of aliases, whose expansion can hide path
overrides. Installation builds must name a local `--path` producer so Cargo can
discover that producer's configuration; registry/git installation requests must
first supply their local source. `install --list` remains a read-only query.
The installer's mandatory proxy suite exercises root paths, escaping symlinks,
other jobs' paths, config discovery and the former uncached-launch bypass.

The shared cache policy lives only in `admission_policy.py`: 50G under
`/mnt/bb/sccache`. `bb-job.py`, native admission and `sccache-rustc` use the
same `bb-cache.py` authority. A caller disagreement, absent authority or unknown
server policy is CACHE_POLICY infrastructure refusal before compiler work;
there is no uncached fallback. The authority validates the actual Unix peer,
executable, boot/birth identity and launch environment on every admission.
The socket parent becomes root-owned before READY, preventing lazy restarts.

Cache activation is a coordinated host migration, after the signed candidate's
full gate proof. Advance the generated WSL `buildbox.mjs` first: removing its
override is compatible with the existing job-owned launcher. Then publish the
complete policy/authority/client generation through the qualified installer.
It holds admission exclusively, drains connected clients (including queued
tickets) and adopted leases, retires the idle daemon and birth-verified legacy
servers, publishes all consumers, and starts the host service before releasing
admission. Partial cache-generation publication refuses before replacing files.
The compiler wrapper cannot switch while an old job owns its private server.
The same migration is available for later host recovery:

```
sudo -n /usr/bin/python3 -B /home/var/bin/bb-cache.py recover
```

This owner command holds the admission upgrade lock, waits for admitted jobs,
drains legacy servers using pidfds and verified Unix peers, and starts the
root-supervised authority through systemd in the host PID namespace. Private
namespace servers end with their jobs. No cache entries are deleted. The
systemd READY notification follows actual peer/policy validation. Jobs never
stop the host server. A noncanonical legacy endpoint refuses migration rather
than guessing a PID. A private job namespace cannot run the host owner command.

Shared server CPU is explicitly unallocated in each job receipt. Only waited
child CPU is attributed there; cumulative shared-server statistics are never
charged repeatedly to concurrent clients. Under 15% free, admission prints
bounded retained quota observations with their age/source before applying the
unchanged 10% safety floor. Those observations are not current physical usage.

Authority requests are atomic credentialled datagrams, with a single private
sequence-packet reply channel. Idle clients allocate no authority descriptors.
The client closes its copy of the sending endpoint after transferring it;
authority death therefore closes the reply channel and immediately refuses
the request rather than leaving a compiler waiting for a lost datagram.

CPU admission uses the existing `bb-admit.py` priority/fair-share authority and
one `CPU_LEASE_SHAPE = (8, 16)` in `admission_policy.py`. No separate scheduler
or budget-test reserve exists. The 128 job CPUs are spread over CPUs 0–191;
the complementary 64 CPUs serve the one host cache. Every admitted command,
including generated Node preparation, is launched inside its root-owned lease
ceiling (`cpuset.cpus` plus `cpu.max`). Only its bounded work subtree is
delegated. Nested commands reuse the kernel-verified lease and tenant. An
environment marker cannot substitute for the protected record and cgroup.
Migration into another job's writable leaf still requires write access to its
root-owned common ancestor's `cgroup.procs`; delegation never grants that
access. The kernel control tests the actual denial and an isolated negative
that grants the missing ancestor permission.

Both services must set `BUILD_SLOT_TENANT=graphcoder` (SDK) or
`BUILD_SLOT_TENANT=cloud` (Cloud) explicitly. Missing labels refuse before heavy
work, with the original argv in an executable recovery command. Generated WSL
and native callers switch together at the coordinator-announced cut-over;
do not install CPU admission before that coordination and full gate proof.
The same exclusive/draining host migration publishes this complete generation.

Dispatch uses a root-peer/birth-verified handshake: the constructor cannot
start the payload before admission durably journals the ticket. A crash before
handoff starts no work; a crash afterward retains the lease through root
custody and pidfd recovery. A slot becomes reusable only after its exact
subtree is empty. Bounded terminal records resolve exit/retirement races.
Final counters and the known payload outcome are durably checkpointed before
removing the empty subtree. The receipt's terminal sidecar is durable before
deleting the lease record, so slot reuse cannot overwrite its only counters.
Recovery can finish retirement if the manager dies after its last `rmdir`
or after terminal publication. Admission startup adopts that custody even
when the retired directory is already absent.
Receipt sidecars retain lease identity, queue time, execution/end timestamps,
and exclusive subtree CPU/pressure counters. Nested totals overlap their
parent lease and must never be added to it. Shared cache CPU stays unallocated.

The installed sccache 0.18.0 supports its own client-side execution mode:
the wrapper's canonical policy enables `SCCACHE_CLIENT_SIDE=1` and an empty
configuration (`SCCACHE_CONF=/dev/null`). It refuses overrides, error logging
and distributed configuration that can disable this containment. Hashing and
rustc therefore remain in the caller's lease; the host daemon handles storage.
See the [versioned upstream architecture](https://github.com/mozilla/sccache/blob/v0.18.0/docs/Architecture.md).
The actual cold-compiler negative runs server-side mode and observes codegen
outside the job lease, while the admitted mode observes it inside that lease.

The initial shape follows the Cloud observation window
2026-10-25 17:15–19:15Z (292 terminal receipts, 13 active jobs). That evidence
has broad unbounded job scopes and missing/mislabelled tenant custody; it does
not prove whole-host CPU saturation or additive per-merge cost. The initial
8×16 shape is a bounded starting policy, not a measured throughput optimum.
Tune the single shape from joined queue waits, per-merge wall time and CPU
counters after the coordinated cut-over; do not claim realized savings before
those measurements exist.

PR baseline admission uses the single selector block in coordinator/pr-gate.sh
immediately after affected-package planning, before runner staging or remote
dispatch. Tests prefer completed exact-base phases, retaining the historical
green coverage anchor only for focused qualification. Clippy and features
require completed evidence from the exact prepared base. Missing evidence
returns exit 5 to the existing baseline queue; only verified publication wakes
that parked request. An incomplete prerequisite never starts a candidate build.
The captured-old control in `baseline-admission.test.py` demonstrates the
previous ordering dispatching before the same real selector refuses admission.

A candidate payload that already finished before baseline admission was moved
retains its original log and receipts. There is currently no canonical
verdict-only continuation API, so preservation alone grants no PASS or merge
and does not authorize automatically rebuilding that candidate.

Remote launch acknowledges through one private pipe after the wrapper has
armed receipt custody and published its observable PID. The wrapper closes
the acknowledgement descriptor before admission or payload work. Slow receipt
recovery therefore keeps launch pending instead of announcing a nonexistent
job after a fixed poll. Death before acknowledgement returns typed
`LAUNCH_INFRA` with the original recovery command. The generated-wrapper suite
exercises the captured former launcher, a blocked recovery barrier and early
wrapper death; no production polling or timeout is added.

Every dispatched or native bb2 job uses `input_closure.py` before build
admission. The client, host scheduler and final executor call the same parser;
there is no adapter-specific list of comparison prerequisites. Opaque shell,
Python and Node jobs must supply `BUILD_SLOT_INPUTS_FILE` or
`BUILD_SLOT_INPUTS_JSON`, or a `<driver>.inputs.json` companion. Direct Cargo
commands declare their source/base closure automatically; reference-consuming
drivers must explicitly list their external inputs. A generated template is
incomplete and cannot admit a job. An empty external-input list is an explicit
producer-owned assertion that all inputs are in the source snapshot or are
produced earlier in this same job, not an inference from script text.

The version-1 declaration has `complete: true`, the full `base_sha` (null only
outside a repository), and an `inputs` array. Each member names an `id`,
`where` (`source` or `bb2`), `path`, producer-qualified `sha256`, and literal
`producer` argv for the phase that creates that input. Use a baseline-only
producer for a missing baseline, never the candidate's full producer. The
consuming argv itself is refused as its own recovery. Source references are
relative to the dispatched working directory; bb2-only references use absolute
paths and are checked on bb2 before ticket acquisition. Both are rechecked
after queue wait, before compilation. All diagnostics go to stderr.

For example, a plugins driver declares its retained main-Clippy log with the
exact baseline-only command `pwsh -NoProfile -File Q:/lanes/bs.ps1 bash
.bb-plugins-consolidated-main-clippy.sh`. The captured actual consumer and
producer are in `scripts/buildbox/fixtures/input-closure/`. Admission does not
run that command; a missing/hash-mismatched input refuses with its name and
that targeted recovery command. Producer owners remain responsible for
complete declarations; admission cannot discover arbitrary script semantics.

The existing source identity includes declared source inputs even when Git
ignores their filenames. Its shared identity key includes the closure, and
the existing upload/forced-file path transports those same bytes. bb2-only
inputs cannot borrow a cached client verdict before host validation. The
closure's base and canonical hash accompany terminal receipts; they do not
qualify a baseline or introduce verdict reuse.

PR gates snapshot the already-selected qualified test/Clippy/feature logs into
the candidate upload and compare against those exact snapshots. `capture`
deduplicates identical proof bytes, retains original commit basenames for the
historical test-anchor policy, excludes proof bytes from Git text conversion and
records a targeted `restore` command. Restoration copies the original retained
proof and checks its admitted hash; it never compiles. The main and focused
baseline producers declare their complete source/base with no external
comparison inputs. Existing baseline selectors still own result completion
and exact-source qualification.

This is a coordinated generation change: install the input parser together
with its client, scheduler/executor consumers and coordinator declarations.
Cloud and opaque-job producers must provide their declarations at cut-over.
Do not install only the refusing native adapter into old callers. The canonical
installer includes the closure suite before publication. Kernel/runtime test
fixtures likewise declare their complete source-only closure explicitly.

The Windows coordinator and Linux admission share the same parser and atomic
JSON publisher. Proof readers compare opened file identities and reject links
and replacement on both OSes; Linux adds its no-follow flag. Windows can capture
proof bytes without importing Linux's rlimit API. Trying to use that unavailable
API still refuses. Isolated runtime tests stage the whole Python worker bundle
through one shared fixture helper, so a new shared import cannot silently leave
one adapter's test worker incomplete.
