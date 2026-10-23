# Build-box runtime and gate dispatch

This directory is the maintained source for the coordinator's bb2 runtime.
`bb-job.py`, `bb-admit.py`, `bb-admit`, `build-slot`, `sccache-rustc`,
`buildbox-receipt.mjs`, `job_exports.py` and the SDK source-copy pruner install
under `/home/var/bin` on bb2. `buildbox.mjs` and the receipt module install in
WSL's `/home/var/bin`. `coordinator/pr-gate.sh` installs as `Q:/lanes/pr-gate.sh`;
`../pr-gate-checks.sh` installs as `Q:/lanes/pr-gate-checks.sh`.
`coordinator/gate-checks.sh` installs as `Q:/lanes/gate-checks.sh`, and
`../gate-test-runner.sh` installs as `Q:/lanes/gate-test-runner.sh`.
Both main and PR dispatch verify the linked libFuzzer driver before replacing
libtest arguments, so an ordinary test with the same basename stays a test.

Before installing, preserve the existing `.bak-gatespeed` backup and take an
immediate snapshot. Compare live hashes with the revision read before editing.
Take `Q:/lanes/pr-gate/.lock` using mkdir only while absent, swap complete files,
and release it immediately. Never interrupt an active gate. Installing a Python
file does not update the admission daemon already in memory: hold its
`/mnt/bb/admit/upgrade.lock` exclusively, let all existing connections and leases
drain, replace the idle daemon, then release the lock. Existing jobs continue;
new ticket handshakes wait in flock.

Each job has private mode-0700 HOME, XDG directories, TMPDIR and targets under
`/mnt/bb/jobs/job-*`. Targets are deleted at exit and on interrupted-job recovery.
Recovery also removes the generated wrapper's outer `.targets` and `.tmp`
directories. A cleanup refusal produces an INFRA receipt and exit status.
Gate phases use that same lifetime. This supersedes persistent gate target
caches. `/mnt/bb/sccache`, installed Cargo/rustup toolchains and
`/mnt/bb/toolchains/cargo-xwin` persist. All wrapper diagnostics go to stderr;
stdout is the child's byte stream. A startup cache failure is explicit but
permits uncached execution. The real-home inventory excludes only the shared
`~/.cache/sccache` directory, and still rejects writes to other real cache/state.

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

Run tooling tests **on bb2**, without compiling Rust:

```sh
python3 scripts/buildbox/runtime.test.py
node --test scripts/buildbox/generated.test.mjs scripts/pr-gate-checks.test.mjs scripts/gate-test-runner.test.mjs
```

They cover JSON metadata stdout with a deliberate stdout-contamination mutant,
binary stdout, mode bits, the restricted inventory exclusion, real leaks,
unreadable-directory cleanup, artifact export, target cleanup after SIGKILL and
its interrupted terminal receipt, age/count pruning and active-copy negatives,
admission disk refusal on fd 2, rolling upgrade, tenant caps/fairness and server
endpoint isolation. Recovery rejects absent/reused process identities and exits
between identity lookup and pidfd creation. Generated-wrapper kill tests include
the previous missing outer cleanup as a negative control. PR dispatch tests prove `--all-targets` and separate doctests
for all selected/reverse-dependent packages, preserve failure status and logs,
and demonstrate the old target selection's false pass. The existing main runner
owns bounded fuzz execution through `../gate-test-runner.sh`.
