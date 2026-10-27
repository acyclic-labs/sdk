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
expiry and output retention. Snapshot, checkpoint restoration and fork currently
refuse: the adapter has not yet bound native `snapshotContainer()` to the shared
checkpoint lifecycle. Cloudflare documents that primitive as an immutable,
point-in-time root-filesystem snapshot, with a 30-day retention period refreshed
by restore. This is distinct from the removed TAR-copy approximation. Images require bash, POSIX
sh, setsid, `/bin/kill`, readable `/proc` and writable `/tmp`. No memory forks,
suspend/resume, idle policy or unrestricted egress are advertised. Provider-default
lifetime resolves to 24 hours; explicit infinite lifetime is refused. Operation records and captured console bytes live in the provider process. Keep that instance alive until cleanup.
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
A previous source revision passed the complete canonical suite on bb2 on
2026-10-26 in 11.97 seconds (log
`/home/var/log/buildbox/1793049401359-116279-5e4947b0.log`). That run exercised
the removed TAR-copy approximation and does not prove snapshot consistency.
Source `19aa611df8207fa2ebfbf1a1e6609b37a3142990` was validated on bb2 on
2026-10-27 (log `/home/var/log/buildbox/1793089603079-134925-7c549c00.log`).
The unchanged shell suite passed through actual workerd/native Containers;
the unchanged lifecycle suite refused its unconditional checkpoint vector.
The combined suite took 11.49 seconds and returned failure. All three unfiltered
library runs passed: Daytona 24, Machines 68 and conformance 49 tests. Worker
TypeScript checking passed. The shared process custodian confirmed workerd's
process tree retired, and the build receipt confirmed job-home, target and
temporary-directory cleanup. No complete lifecycle qualification is claimed.

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
   destroys all observed machines even on a failed vector. Inspect that namespace
   for an uncertain create before rerunning; never replay an unknown mutation.
5. Remove this dedicated Worker deployment after qualification with
   `npx wrangler delete` from its deployment directory and retire the test token.
