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
A previous source revision passed the complete canonical suite on bb2 on
2026-10-26 in 11.97 seconds (log
`/home/var/log/buildbox/1793049401359-116279-5e4947b0.log`). That run exercised
the removed TAR-copy approximation and does not prove snapshot consistency.
Source `07e558968fa4926b9262562885b5b48be2620de0` was validated on bb2 on
2026-10-27 (job `1793091808662-142237-afe1e7af`, log
`/home/var/log/buildbox/1793091777813-142237-e0de4a9f.log`). Both unchanged
lifecycle and shell suites passed against actual workerd/native Containers in
14.88 seconds. All unfiltered library runs passed: Daytona 24, Machines 72 and
conformance 49 tests; compile-only and Worker TypeScript checks also passed.
The separate native snapshot probe restored old child bytes after a parent write.
Its [unaltered response capture and provenance](../../rust/crates/machines/tests/fixtures/cloudflare/README.md)
records eleven actual exchanges. The shared process custodian confirmed workerd's
process tree retired; the build receipt confirmed all job-home, target and
temporary-directory cleanup. Hosted Cloudflare qualification remains a live step.

Source `d553734e82cecf83244596872a4e8285ad03e2a0` also passed the unchanged
lifecycle and expanded shared shell suites against workerd in 16.76 seconds,
including omitted timeout and long UTF-8 source split across argument boundaries.
Both native and machine shell admission now uses 1..60 seconds (default 60) and
1..16384 source bytes; this reduces the prior native shell allowance.
All unfiltered libraries passed (Daytona 24, Machines 73, conformance 49), as did
Worker TypeScript, Machines WASM build, TypeScript compilation and all 53 Bun tests.
Job `1793093958321-149476-5f8c6e43`, log
`/home/var/log/buildbox/1793093944726-149476-81513db7.log`, completed successfully;
workerd was positively retired and the receipt confirms job cleanup.
The separate full harness-core library attempt reported 23 failures and stalled
in a durable-host test until bb2's existing hang reaper killed it. No complete
harness-core suite pass or main-baseline equivalence is claimed.

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
