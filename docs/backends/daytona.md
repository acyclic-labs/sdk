# Daytona Machines

Compose `DaytonaProvider::new(config, OperationRegistry::new(streams, account)?)`.
Supply the application's authenticated canonical Streams backend; the account
segment grants no authority. Reopen the same store after restart. Successful
same-key requests return the original outcome and settlement CommitId; a pending
or unknown operation never authorizes another native dispatch.

Register a Daytona snapshot and its enforced `core::NetworkPolicy` in
`DaytonaConfig`; network commitments use their exact 32-byte digest. Shared
admission checks the enrolled domains before the adapter encodes Daytona fields.
VM-class snapshots support native pause/resume, memory checkpoints and live fork.
Containers do not advertise an atomic fork boundary. Fixed-instant expiration,
spend and concurrency budgets that Daytona cannot enforce are refused. API
endpoints must be HTTPS or loopback HTTP, with no URL credentials, query or
fragment. Credentials are admitted once; redirects and automatic HTTP retry are
disabled. Toolbox destinations use shared credential endpoint admission with the
configured parent origin and proxy host enrollments.

Guest execution prepares a lazy toolbox v2 process-ledger stream. The shared core
owns separate binary stdout/stderr, exit status, output allowance, drop
cancellation, process-group cleanup and WallAlarm expiry. Retained command output
is the core's bounded ephemeral console, not restart recovery. An image without
the process-ledger API refuses guest execution. Internal vectors do not establish
that the registered provider image supports this API.

The G29 bb2 checks cover shared canonical settlement/reopen, driver affinity,
cleanup ownership, exact replay and internal execution/transport vectors. The two
billable provider tests remain ignored and were not run. See the PR's exact-source
receipts; no live streaming or provider qualification is inferred. For live
validation, the coordinator must supply an account-specific durable
`DAYTONA_MACHINES_JOURNAL`, a registered `DAYTONA_SNAPSHOT` and `DAYTONA_API_KEY`,
run the existing live suite and guest vectors, then reopen the same journal and
verify exact settlement without a second allocation.
