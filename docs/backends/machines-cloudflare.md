# Cloudflare Machines

Enable `acyclic-machines/cloudflare`. Deploy the transport in
`deploy/machines-cloudflare`, then compose
`RemoteMachines::new(Cloudflare::new(worker_url, token)?, config)` from
`acyclic_machines::provider_core`. Register the deployed `workspace` image under
its immutable application digest in `config.images`, register `NetworkPolicy::BlockAll`
under its policy digest in `config.networks`. Supply `/workspace` to the qualification
runner. Hosted transports require `TRANSPORT_TOKEN`; loopback workerd permits
account-free access.

The Worker uses native Container exec and Sandbox 1.0 binary Files. The shared
core owns lifecycle admission, replay, process-group cancellation, WallAlarm
expiry and output retention. Native `snapshotContainer()` supplies an immutable,
point-in-time root-filesystem generation for checkpoint, restore and disk fork;
no file-copy approximation is used. The core owns checkpoint admission, expiry,
operation cancellation and child custody. Cloudflare retains snapshot data for
30 days, refreshed by restore. Destroying an SDK checkpoint revokes further forks;
the Worker API exposes no physical snapshot deletion. Images require bash, POSIX
sh, setsid, `/bin/kill`, readable `/proc` and writable `/tmp`. No memory forks,
suspend/resume, idle policy or unrestricted egress are advertised. Provider-default
lifetime resolves to six hours; explicit infinite lifetime is refused. Operation records and captured console bytes live in the provider process. Keep that instance alive until cleanup.
This workspace binding does not implement the harness kernel filesystem seam.

## Local validation

Run on bb2 from this worktree:

```powershell
pwsh -NoProfile -File Q:/lanes/bs.ps1 bash -c 'bash scripts/qualify-machines-cloudflare.sh'
```

This runs the canonical Machines suite through actual workerd and native local
Containers, including binary stdout/stderr before exit, exit 7, descendant
cancellation on stream drop and quiet absolute expiry. It installs the pinned
Worker dependencies and checks TypeScript. No response fixtures are synthesized.
Source `5deac2f509b0d9581bf2a1e6d2a713d249982a12` passed on bb2:
compile gate, full conformance 24 / Machines 77 / Daytona 49 libraries,
actual workerd lifecycle and shell suites (15.77 s together), native snapshot
probe and Worker TypeScript. The captured overlay contained backend docs only.
The custodian confirmed workerd retirement; all four build cleanup fields are
true. Job `1793097182725-162966-0c28c563`, log
`/home/var/log/buildbox/1793097167860-162966-73b62143.log`, rc0, wall547.672s
including compilation. Source `01e90ce7cc56997b6d282933639e2f10d89d9e2c`
also passed the full public native shell contract (2 tests). Hosted account
validation remains a live step.
Later shared RPC cleanup source `25830279bd2648d01670c6b6ebe00176967426b6`
passed the compile gate and the same full library suites, with no new workerd
run: job `1793098004380-166798-be025746`, rc0, all cleanup fields true.

MachineShell shares the original native input schema: omitted timeout300s and
positive checked-u32 millisecond deadlines. Supply an explicit timeout within
1..60s for Machines: its canonical command admission refuses the longer native
default before dispatch. The same suite checks omission, explicit120s and
oversized argument refusal without guest mutation, then executes supported
multibyte source across argument boundaries. Native acceptance is preserved.

The [checked-in native snapshot capture and provenance](../../rust/crates/machines/tests/fixtures/cloudflare/README.md)
contains eleven unaltered real responses from earlier source07e55896. Later
snapshot probes also restored old child bytes after a parent write. Historical
TAR-copy results are not snapshot consistency evidence. A separate full
harness-core attempt reported23failures and stalled until bb2's hang reaper
stopped it; no complete harness-core or main-baseline pass is claimed.

Native snapshot contract: [Cloudflare Container API](https://developers.cloudflare.com/containers/api/durable-object-container/)
and [snapshot retention](https://developers.cloudflare.com/containers/guides/snapshots/).

## Live steps

1. In an isolated Cloudflare account with Containers enabled, install the pinned
   dependencies with `cd deploy/machines-cloudflare && npm ci --ignore-scripts`.
2. Authenticate Wrangler in the coordinator's credential environment. Run
   `npx wrangler secret put TRANSPORT_TOKEN`, entering a fresh transport token.
3. Run `npx wrangler deploy`; retain its deployment version and container image
   digest. Export `CLOUDFLARE_MACHINES_TRANSPORT_TOKEN` securely in the Rust test environment.
4. From the repository root run
   `cargo run --locked -p acyclic-conformance --features cloudflare,machines-shell --example cloudflare-conformance -- https://WORKER_HOST`.
   Preserve the printed namespace, complete output and elapsed time. The runner
   destroys all observed machines and revokes all SDK checkpoint references even on a failed vector. Native snapshot data follows provider retention. Inspect that namespace
   for an uncertain create before rerunning; never replay an unknown mutation.
5. Remove this dedicated Worker deployment after qualification with
   `npx wrangler delete` from its deployment directory and retire the test token.
