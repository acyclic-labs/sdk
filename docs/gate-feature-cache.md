# Gate feature-matrix artifacts

`gate-feature-matrix.sh` runs the reviewed matrix on every invocation. Inside
a proven private buildbox mount, `gate-target-cache.py` seeds the worker targets
from a completed immutable matrix snapshot, then executes the same Node/Cargo
commands. The matrix footer and exit status are newly computed; no verdict is
reused. Mutable targets remain job-owned and are removed by the runtime.

Every worker target is inside the matrix root (`worker-0`,
`worker-0-features-1`, and so on). The existing target store owns the exact
source/argv/environment/toolchain/target key and completed publication. This
adapter does not change those keys, compiler flags or package/feature sets.
Changed source or toolchain requires a new request. Cargo still validates all
fingerprints in a restored target.

The Windows matrix uses its own private root rather than inheriting the Linux
matrix's occupied target. `with-xwin-cache.py` prepares stable SDK/compiler paths
inside the private target mount, with one completed workspace per target and
worker count. The runtime retains that workspace until the job's children exit.
Initialization uses the shared hierarchical lease; an incomplete workspace is
refused without deleting or repairing a possibly live worker. Legacy callers
without private mount proof retain temporary SDK workspaces.

Each SDK snapshot records the seed's file inventory (including inode/ctime)
under its existing seed lock and refuses a change during copying. The combined
SDK identity remains in the strict environment key, so stable paths cannot hide
an SDK change. The compiler copy must match its reviewed hash before a completed
workspace can be reused. This uses the existing SDK snapshot factory rather
than adding another downloader or seed store.

The private source clock normalizes directory times as well as file times.
Cargo build scripts can watch entire `proto` or `data` directories: identical
files in a newly created directory otherwise rerun those scripts and rebuild
their consumers. Directory times derive from the uploaded descendant paths and
blobs, after dependency preparation. This applies only to the exact private
uploaded tree; subsequent job writes retain their real timestamps. The generated
clock control demonstrates that the former files-only clock leaves a fresh
watched directory, while changed directory inputs produce a different time.
Dependency preparation precedes tree-identity verification too: a package
script that changes source must fail that verification before any clock can
qualify the changed bytes under the original uploaded identity.

The adapter acquires the production hierarchical target lease only while
cloning or publishing. It releases that parent lease before any Cargo worker
starts, so descendant workers can acquire their own leases and siblings can
compile independently. The existing matrix flock covers the complete run;
the job's runtime lease keeps its target and temporary directories alive.
A separate job-local matrix lifetime lease is inherited by the Node scheduler.
It survives a killed Python launcher until no scheduler can start more writers;
already running Cargo children retain the production hierarchy leases.
Only successful matrices with unchanged uploaded inputs publish. Failed,
interrupted or source-mutating runs retain their actual nonzero status and
cannot publish a new completed snapshot. Snapshot failures log diagnostics
and execute the checks without a seed. Receipt persistence failures are typed
runner-INFRA. Snapshot diagnostics go to stderr, preserving child stdout.

Canonical PR test/clippy target preparation uses the same receipt plus actual
mount proof and `CARGO_INCREMENTAL=0`. It creates empty targets without a
legacy clean or boot/inflight markers. Outside that proof it retains the
existing boot/interrupted-build protection. `BB_JOB_ACTIVE` alone is never
accepted as proof.

Runner preparation also preserves uploaded source metadata. An already
executable runner is used unchanged; a non-executable uploaded runner is copied
atomically to job temporary storage and made executable there. An unconditional
`chmod +x` changes source ctime even if its mode already includes execute bits,
which correctly makes the source-clock oracle reject subsequent reuse. The
fix preserves the oracle instead of ignoring ctime changes.

The shared source-clock guard also compares the current Git tracked/untracked
inventory with uploaded names, using the exact upload pathspec and filter
carried by the clock producer. Added or renamed source inputs cannot publish
under the original key. Existing metadata, ctime and content checks remain.
The namespace-aware store generation rejects earlier snapshots. Gate metadata,
artifacts, diagnostics and statuses live in private job temporary storage, so
generated outputs do not pollute the source inventory. Legacy runners retain
their existing output location when private custody is absent.

Privileged tooling controls pass Python's `-B` flag across sudo rather than
depending on an inherited environment variable. Importing an installer must
not create root-owned bytecode in an uploaded checkout and prevent its later
retirement. Source-copy retirement keeps one stable tomb name and the original
source lease, including after an incomplete removal; another admission cannot
rename that tomb repeatedly or bypass the original lease. The installer requires
the independent retirement controls before publishing tools.

Controls run on bb2:

```sh
python3 scripts/gate-target-cache.test.py
python3 scripts/with-xwin-cache.test.py
python3 scripts/buildbox/cargo_cache.test.py
python3 scripts/buildbox/install.test.py
python3 scripts/buildbox/prune.test.py
node --test --test-concurrency=1 scripts/pr-gate-checks.test.mjs scripts/main-gate-checks.test.mjs
```

These include real private-mount proof, flag-only/ordinary-directory negatives,
empty production PR preparation versus the old marker/clean path, actual XFS
reflink publication and clone across worker targets, execution after restore,
nonzero/signal and source-mutation refusal, full-key changes, and hierarchical
parent/child/sibling exclusion. Real gate measurements belong in the PR report;
fixture timings do not establish fleet savings or an eight-second full matrix.
