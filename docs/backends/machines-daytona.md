# Daytona Machines

Compose `DaytonaProvider` from the `acyclic-machines-daytona` crate with an
explicit `DaytonaConfig`; the crate README describes image/network registration
and the existing VM versus container capabilities. The provider now uses the
shared Machines operation registry, allocation receipts and
execution owner. Those shared records remain process-local except for Daytona's
existing labeled resource recovery. VM snapshots/forks use Daytona's native boundary.
Container disk fork is refused until an atomic provider snapshot or an admitted
immutable filesystem generation is available; two matching TAR scans cannot
establish snapshot consistency.

Exact binary stdout/stderr streaming requires Daytona's
[v2 process ledger](https://www.daytona.io/docs/en/v2/process-execution/),
currently documented as alpha. Register a snapshot exposing that toolbox API,
with bash, POSIX sh, util-linux setsid, `/bin/kill` and readable `/proc`. An older toolbox
fails admission explicitly. Process-group cancellation and WallAlarm command
expiry use the same core as the other remote machines. The existing synchronous
toolbox API is retained for the provider's internal lifecycle/file operations.

## Validation and Live steps

Existing recorded Daytona lifecycle responses keep their original provenance in
the crate's fixtures. Unit regression checks run on bb2 with the Machines tests.
The new process-ledger streaming transport has compiled but has not yet been
validated against a real v2 toolbox or recorded real stream.

The coordinator should supply `DAYTONA_API_KEY` privately, select an isolated
organization/tenant and set `DAYTONA_SNAPSHOT` to a v2 Linux VM snapshot. Run
`cargo test --locked -p acyclic-machines-daytona --test live -- --ignored`.
Retain the complete output and sanitized process-create/SSE/delete captures with
toolbox version, image identity, capture date and hashes. Inspect the tenant's
resources after any interrupted test and delete its test sandboxes and snapshots
before rerunning. Never replay an uncertain create.
