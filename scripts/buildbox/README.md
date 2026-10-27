# Build-box runtime and gate dispatch

Script-driven compiler producers explicitly opt in to the same policy as direct
Cargo with `pwsh -NoProfile -File Q:/lanes/bs.ps1 --env BUILD_SLOT_COMPILER=1
--env BUILD_SLOT_PR=NUMBER --env BUILD_SLOT_PHASE=LABEL bash DRIVER.sh`. This is
the source contract for the coordinated runtime cut-over, not permission to
install a private compiler/cache. The single `admission_policy.compiler_request`
decision admits the wrapper before execution; the generated transport preserves
that admitted environment. Other tools retain their own compiler capture setup.
Declare and validate all producer/reference inputs through `input_closure.py`
before dispatch. Keep outputs under job TMPDIR until compilation finishes;
changing uploaded source refuses target snapshot adoption/publication. Receipts
report the exact source/argv/toolchain and sccache counters. Cargo `fresh=false`
alone says nothing about sccache hits. Reserve transport/export time within the
600-second total job budget; a remote ceiling of 510 seconds is an upper bound,
not a prediction that an unmeasured graph fits it.

The same prerequisite parser has a public `run` boundary for script-driven
consumers. Set `BUILD_SLOT_INPUTS_FILE` to the complete captured manifest, then
invoke `python3 -B scripts/buildbox/input_closure.py run --root "$PWD" --base
FULL_BASE_SHA -- bash DRIVER.sh`. It admits the entire declared closure before
executing the command, writes `BB_INPUTS_ADMITTED` only to stderr, and preserves
the child's stdout and terminal status. Client `validate --stage client`
remains the pre-transport check; on-box `run` checks again before any compiler
side effect. This works with a byte-identical, source-pinned helper artifact
before the new automatic host generation is installed; it does not imply that
the older installed build-slot enforces a manifest automatically.

A reference declaration binds qualified bytes, not a headline verdict. Include
the exact producer source/command/features/target/toolchain proof alongside each
comparison log. A workspace Clippy receipt cannot establish equivalence to a
different named-package invocation. Missing producer custody is a prerequisite
refusal, never a reason to rebuild an already-qualified candidate.

The worker records its private mount namespace and releases custody without
walking job HOME. The existing terminal receipt owner first reaps the worker,
then performs cleanup from the outer namespace. Cleanup from the recorded
private namespace is a typed refusal, including after disconnected FUSE tests.

This directory is the maintained source for the coordinator's bb2 runtime.
`bb-job.py`, `bb-admit.py`, `bb-admit`, `build-slot`, `sccache-rustc`,
`buildbox-receipt.mjs`, `job_exports.py` and the SDK source-copy pruner install
under `/home/var/bin` on bb2. `buildbox.mjs` and the receipt module install in
WSL's `/home/var/bin`. `coordinator/pr-gate.sh` installs as `Q:/lanes/pr-gate.sh`;
`../pr-gate-checks.sh` installs as `Q:/lanes/pr-gate-checks.sh`.
`coordinator/gate-checks.sh` installs as `Q:/lanes/gate-checks.sh`, and
`../gate-test-runner.sh` installs as `Q:/lanes/gate-test-runner.sh`.
`coordinator/gate-test-baseline.py` and `coordinator/failed-tests.sh` install
under `Q:/lanes` with their existing names. Focused main qualification uses
the same all-targets/doc coverage and fuzz helper as PR dispatch; non-libtest
failures retain package/target identity for comparison.
Main, PR and focused-baseline dispatch compile with --no-run and collect Cargo
JSON compiler-artifact executables qualified by fuzz package id and bin kind.
Only exact full executable paths receive deterministic fuzz arguments; an
ordinary test with the same basename retains libtest arguments. Discovery
finishes before execution, avoiding a JSON-consumer/runner race. The second
Cargo invocation executes already-built targets without rebuilding them.

Install only through `bb-install.py` (see `docs/buildbox-install.md`): it
publishes whole generations behind one `current` link, canaries them before
and after the switch, and switches back on failure. Take `Q:/lanes/pr-gate/.lock`
using mkdir only while absent and release it immediately afterward. Never
interrupt an active gate. The installer holds `/mnt/bb/admit/upgrade.lock`
exclusively and retires the idle daemon, since a loaded daemon keeps its old
code; new ticket handshakes wait in flock.

For a client/runtime protocol change, install the client first and let every
pre-install client process finish before installing the bb2 runtime. New
clients may use the prior runtime during this handoff; old clients must never
be launched against a runtime requiring the new protocol. Do not stop jobs.

Each job has private mode-0700 HOME, XDG directories, TMPDIR and targets under
`/mnt/bb/jobs/job-*`. Targets are deleted at exit and on interrupted-job recovery.
Recovery also removes the generated wrapper's outer `.targets` and `.tmp`
directories. A cleanup refusal produces an INFRA receipt and exit status.
Gate phases use that same lifetime. This supersedes persistent gate target
caches. `/mnt/bb/sccache`, installed Cargo/rustup toolchains and
`/mnt/bb/toolchains/cargo-xwin` persist. All wrapper diagnostics go to stderr;
stdout is the child's byte stream. A startup cache failure is explicit but
permits uncached execution. A recursive read-only bind mount protects all
of real `/home/var` in a private mount namespace inherited by every child.
Each entry, including nested build-slot, also owns a PID namespace with a fresh
proc mount: other processes' `/proc/<pid>/root` aliases cannot expose a writable
host view of HOME. The constructor is unconditional; environment markers cannot
bypass it. Kernel mount flags and private proc identity are verified.
The inherited working directory is reopened through the protected namespace;
relative paths cannot retain the old writable HOME mount.
Only the job process tree is restricted; concurrent tenant writes cannot fail
this job. No shared inventory or path exclusions remain. Unsupported kernels
and all inherited real-HOME file descriptors refuse execution as INFRA. Even read-only handles can otherwise reopen their old mount through procfs.
The terminal receipt records the enforced guard. Installed tools
are readable, and Cargo uses writable `/mnt/bb/toolchains/cargo`. Provision
toolchains and registry caches outside the job before qualification; job commands
cannot install into the protected rustup directory. Buildbox gives each job
private Git refs/index/new objects and bind-mounts its .git pointer only inside
its existing private worktree namespace. Shared mirror objects remain read-only
alternates. The host worktree and pointer are unchanged; an unsafe host-namespace
mount refuses execution. Other writable trees remain
accessible. The guard preserves setuid `fusermount3`, required by native checkout
tests; `no_new_privs` would disable that privilege transition and is refused.
Each remote source copy includes the unique job identity, so a WSL restart and
released client claim cannot reset another detached job's checkout.

Receipt and signaling ownership includes the PID namespace as well as PID birth
identity. An invisible foreign PID is held, never presumed dead. The constructor
registers each namespace with its outer receipt; after that owner has terminated
the namespace, its terminal callback finalizes interrupted descendant receipts.
Unproven termination defers cleanup instead of deleting another live job's files.
Receipt publication and ledger appends use kernel `flock` custody, released even
after SIGKILL. Unknown legacy directory-lock owners are neither observed nor
stolen and cannot block these writers. Missing parent metadata never proves that
a foreign namespace ended. The namespace tests include legacy locks, a killed
kernel writer and concurrent receipt completion with one ledger entry.
Admission's live PID-birth lease can also certify an independently written job
record; a foreign namespace or an unobservable daemon never grants that authority.
Cleanup takes an exclusive kernel lease; the running job holds the shared lease
through its lifetime. The same lease protects its outer target and temporary
directories. Target and TMP admission checks filesystem device identity, including
bind mounts and symlinks, and refuses the root filesystem.

The remote wrapper archives declared `BUILD_SLOT_REMOTE_FETCH` outputs before
target cleanup. The client extracts those archives and removes them after
copy-back. A failed export is INFRA. No target cache is retained to support
copy-back. Glibc-pinned cross builds map the remote target-triple directory back
to the caller's requested local profile path; missing declared paths fail export.
Native callers must export artifacts they want to retain before
returning. SDK source copies use the newest directory/index/HEAD mtime for the
12-hour age limit, plus a cap of 128 copies. Leased or live legacy copies are
preserved. If active copies prevent meeting the cap, admission refuses with an
INFRA diagnostic on stderr. Disk admission requires 10% free on both filesystems.

Run tooling tests **on bb2**; their Rust fixtures compile there too:

```sh
python3 scripts/buildbox/runtime.test.py
python3 scripts/buildbox/baseline.test.py
python3 scripts/buildbox/coordinator/main-prepare.test.py
node --test scripts/buildbox/generated.test.mjs scripts/pr-gate-checks.test.mjs scripts/gate-test-runner.test.mjs
node --test scripts/buildbox/receipt-namespace.test.mjs
```

They cover JSON metadata stdout with a deliberate stdout-contamination mutant,
binary stdout, mode bits, rejected real-HOME writes through symlinks and children,
concurrent outside writers, missing kernel enforcement and inherited descriptors,
unreadable-directory cleanup, artifact export, target cleanup after SIGKILL and
its interrupted terminal receipt, age/count pruning and active-copy negatives,
admission disk refusal on fd 2, rolling upgrade, tenant caps/fairness and server
endpoint isolation. Recovery rejects absent/reused process identities and exits
between identity lookup and pidfd creation. Generated-wrapper kill tests include
the previous missing outer cleanup as a negative control. PR dispatch tests prove `--all-targets` and separate doctests
for all selected/reverse-dependent packages, preserve failure status and logs,
and demonstrate the old target selection's false pass. The existing main runner
owns bounded fuzz execution through `../gate-test-runner.sh`.

SDK artifact snapshots use exact uploaded inputs and private targets. Blob-derived
source mtimes survive fresh source copies so Cargo fingerprints can remain fresh;
all inputs are rehashed before restore and publication. Changed files, including
changes that restore the old mtime, prevent publication under the original key.
External source symlinks bypass snapshots. This policy never applies to mutable
targets shared across source trees, and it does not cache gate verdicts.

Main preparation validates the designated Git/common directories and checks every
checkout, clean and runner copy. HEAD and the copied runner are verified again
before publishing results. The attempted commit is persisted before preparation
or dispatch, so a coordinator restart cannot forget an infrastructure hold.
Only a changed commit or explicit `.rerun` authorizes that held input again.

Receipt admission arms only its own metadata and watcher. Historical recovery
is an explicit maintenance operation (`node buildbox-receipt.mjs recover DIR`),
not a prerequisite of client, remote-wrapper or native admission. It must not
be invoked by each producer. SDK source retirement takes its nonblocking global
maintenance lease before inspecting host processes; a concurrent caller reports
`maintenance=owned-by-peer` with unknown capacity, rather than repeating that
traversal or certifying the peer's result. Per-source leases and disk admission
remain authoritative for the admitted job.
