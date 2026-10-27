# Linux virtio-fs

`acyclic_fs::native_mount::GuestSessionAdmission` exposes raw Linux FUSE packets
and standard vhost-user-fs over a Unix socket. The host `/dev/fuse` mount and
virtqueues use one packet dispatcher and the existing projection state.
QEMU and cloud-hypervisor require shared guest RAM; DAX is unnecessary.

## Composition

Create an explicit `hosting::Bound::local_root` with the deployment's original
issuer, `Placement` and `Turns`. The placement retains the canonical `Fs` and
`Bound` derives its agent stream from that same filesystem.
`Bound::guest_session(enrollment, work)`
opens its exact canonical volume, reuses the signed policy compiler and source
guard, and returns the sole authority input to the packet/socket entries.
`LocalGuestEnrollment` contains the volume reference, writable intent and a
source-owned POSIX identity map plus allowed metadata mutations. Identity pairs
are identity mappings into virtual volume metadata, never host user selection.

```rust,ignore
let admission = bound.guest_session(local_enrollment, &original_work).await?;
let retired = admission.serve(already_connected_unix_stream, b"acyclic")?;
if let Some((cause, counters)) = retired.take_callback_failure() {
    return Err(cause.into());
}
retired.transport()?;
```

Embedding a different trusted local composition can implement `LocalGuestSource`.
It must retain the authenticated canonical source and its original funded work,
guard, exact view and enrolled POSIX policy together. This is a source-owner
boundary, not a guest-request interface. Session consumers cannot supply a UID,
path or cache flag. `packet` retains original work/view leases through its
physical publisher closure; a failed/uncertain completion closes the session.

The standalone command uses the SDK's existing protected local-owner enrollment
and the same `Bound`, with no model, shell tool or provider credentials:

```sh
cargo build --locked -p acyclic-harness --features filesystem-local,native-execution \
  --bin acyclic-virtiofsd
acyclic-virtiofsd --socket /tmp/acyclic-fs.sock --config /srv/acyclic/guest.json
```

The local JSON has `directory` (canonical local filesystem store),
`state_directory` (protected local owner state), `enrollment` and `budget`.
Serialize the enrollment with the SDK so its `volume` remains the exact existing
`VolumeRef`; use `GuestPosixPolicy { identities: vec![(0, 0), (1000, 1000)],
metadata: true }` only when the local owner enrolls those identities and metadata
mutations. Serialize a finite `acyclic_fs::performance::work_budget(ops, bytes)`
as `budget`; omitted counters have zero allowance. The command opens the enrolled
volume, does not create a substitute project volume, and needs no `/dev/fuse`.
The existing source must support the intended native POSIX metadata operations.
The integration test `harness/tests/virtiofs.rs` shows complete local composition
and config generation. Ctrl-C cancels the original work and joins the server;
the daemon removes only its own socket inode.

## Limits

Hosted `Bound::admitted` refuses guest admission before volume IO. Linux can
satisfy later private-mapping faults without a server callback. Completed INVAL,
direct I/O and zero TTLs cannot establish revocation of those private mappings.
LocalRoot enrollment does not substitute for a verified hosted parent.

This transport chooses direct I/O, zero name/attribute/negative/symlink/directory
TTLs, and no KEEP_CACHE/writeback. Source callbacks retain the original guard.
Unmapped UID/GID and forbidden metadata operations refuse before volume IO;
chown targets must also be enrolled. Device-node creation is unsupported.

Two split queues (hiprio plus one request queue), indirect descriptors,
EVENT_IDX, CONFIG/MQ/REPLY_ACK are supported. DAX, notification queues, migration,
inflight restoration and device-state transfer are not advertised. Maintained
rust-vmm crates supply the protocol, queue and memory primitives. Socket failure
never replays an uncertain filesystem mutation. Every started physical worker
joins before source or guest-memory mappings are released.

## Standard frontends

Use a trusted kernel/initramfs containing virtio-fs and a shell. Start the daemon
above, then choose one frontend:

```sh
qemu-system-x86_64 -enable-kvm -nodefaults -nographic -serial mon:stdio \
  -m 512M -object memory-backend-memfd,id=mem,size=512M,share=on \
  -numa node,memdev=mem -kernel "$GUEST_KERNEL" -initrd "$GUEST_INITRAMFS" \
  -append 'console=ttyS0 rdinit=/bin/sh' \
  -chardev socket,id=fs,path=/tmp/acyclic-fs.sock \
  -device vhost-user-fs-pci,chardev=fs,tag=acyclic,queue-size=512

cloud-hypervisor --kernel "$GUEST_KERNEL" --initramfs "$GUEST_INITRAMFS" \
  --cmdline 'console=ttyS0 rdinit=/bin/sh' --memory size=512M,shared=on \
  --serial tty --console off \
  --fs tag=acyclic,socket=/tmp/acyclic-fs.sock,num_queues=1,queue_size=512
```

Inside either guest:

```sh
mkdir -p /mnt/acyclic
mount -t virtiofs acyclic /mnt/acyclic
printf 'virtio-fs qualification\n' > /mnt/acyclic/qualification
cat /mnt/acyclic/qualification
rm /mnt/acyclic/qualification
test ! -e /mnt/acyclic/qualification
umount /mnt/acyclic
```

Upstream Firecracker exposes vhost-user block, not a filesystem frontend; its
block API cannot attach this socket. A build must expose standard device ID 26
and a vhost-user-fs frontend before there is a valid filesystem command to run.
No private Firecracker variant is supplied. References:
[QEMU](https://www.qemu.org/docs/master/system/devices/virtio/vhost-user.html),
[cloud-hypervisor](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/docs/fs.md),
[Firecracker](https://github.com/firecracker-microvm/firecracker/blob/main/docs/api_requests/block-vhost-user.md).

## Validation

After rebasing onto main `d8224faf`, runtime source
`708c878c16db5e78e1f4ee0a5f7a7492d5d33860` uses the current public
`Bound::local_root` producer with the original issuer, canonical `Fs` placement
and real local turn executors. The filesystem/harness/harness-core test and binary
compile gate passed. The complete Bound agent suite passed **30/30**, 0.70 s.
The public daemon suite passed **2/2**, 1.51 s: both actual QEMU and
cloud-hypervisor guests mounted the canonical volume, wrote/read/deleted,
unmounted and powered off with exit 0. Both daemons exited 0 and retired their
owned sockets. Frontends still emitted teardown disconnect warnings.

The unfiltered filesystem run, including ignored integration cases, ended
**2496 pass / 15 fail / 0 ignored / 0 filtered**, 30.33 s. All tests under
`native_mount::fuse` and the Linux mount behavior cases passed. Five failures
remain elsewhere under `native_mount`: pending mutation revocation, rejected
composition retry, source-guard cancellation identity, held-parent fence location
and cumulative publication conflict budget. These recur from the earlier
owner-routed receipt; their files have no lane edits. Ten additional failures
cover device drain, live/publication generation fences, canonical history,
source authority fences, stream error categories and exact staging work. This is
not a passing complete filesystem suite or proof of baseline equivalence.

Current bb2 job `1793094000109-149673-a133e23b`, log
`/home/var/log/buildbox/1793093988453-149673-84bd0baa.log`, overall rc 101,
canonical wall 1017.811 s including compilation. All four cleanup fields are
true. This job did not rerun the harness library suite; its historical failures
below remain source-specific.

The initial complete bb2 run uses signed source
`41955b03d096e0c8d7a180619dbba35dae40905a`:

```powershell
pwsh -NoProfile -File Q:/lanes/bs.ps1 bash -c 'bash scripts/validate-virtiofs.sh'
```

All three compile gates passed. Complete runtime results at that source:

- Filesystem: **2483 pass / 22 fail / 0 ignored / 0 filtered**, 30.35 s.
  Every `native_mount::fuse` test, Linux mount behavior case and private fixture-source
  VMM passed, including revoked/missing-guard/identity/metadata refusals.
- Harness library: **133 pass / 2 fail**, 1.73 s. The failures are oversized
  command-result omission and restrictive checkout-policy setup.
- Bound agent suite: **29 pass / 0 fail**, 0.22 s.
- Public Bound/standalone daemon: **2 pass / 0 fail**, 1.38 s. Both QEMU and
  cloud-hypervisor mounted the actual canonical local volume, wrote/read/deleted,
  unmounted and powered off exit 0. Both daemons exited 0 and retired their owned
  sockets. Frontends emitted teardown disconnect warnings after session retirement.

Overall rc **101**; this is not a passing complete suite. The filesystem and
harness failures are owner-routed, without claiming main-baseline equivalence.
Canonical cleanup confirms home, targets, outer targets and outer temporary
storage removed. bb2 job `1793086189051-119691-9ce6908e`, log
`/home/var/log/buildbox/1793086155710-119691-79f2abab.log`; canonical wall time
1851.215 s includes compilation.
The script compiles first, then runs complete relevant suites without name
filters, including real host FUSE/kernel replay and both actual VMMs through
the public standalone daemon. Refusal vectors check revoked guards, wrong
providers, unmapped identities and forbidden metadata operations before IO.
Captured kernel packets and their provenance are in `filesystem/fixtures/fuse`;
negative vectors explicitly perturb those real packets rather than invent
provider/kernel fixtures. Guest artifact preparation records actual distribution
kernel/module, static BusyBox, initramfs and official VMM release digests.

Review correction `1cf95a1d3355b55a521b7216ee651068a5d7e61b` separates kernel
INIT/DESTROY/reference retirement from caller filesystem identity checks. A
non-root-only enrollment can negotiate the root-tagged kernel INIT without
granting root filesystem access; negotiation still checks the original guard.
Its bb2 compile gate passed, and the complete default filesystem library run
ended **2456 pass / 21 fail / 30 ignored / 0 filtered**, 30.35 s. All five guest
admission vectors passed, including non-root negotiation, root lookup refusal
without source IO and revocation before INIT. Job
`1793088963510-132230-c703d139`, log
`/home/var/log/buildbox/1793088947516-132230-fb799c14.log`, overall rc 101,
canonical wall 632.757 s and all four cleanup fields true. Ignored host/guest
integration tests were not rerun at that correction; the actual VMM results
above remain specific to `41955b03`. Artifact-preparation repeatability is
tracked in [#1679](https://github.com/acyclic-labs/sdk/issues/1679).

Historical `cb33ccdecaac7dd39faa19c0573bcb1dfa99339b` passed the FUSE vectors and
both fixture-source guests, but its complete filesystem suite was **2480 pass /
21 fail**, with earlier intermittent mount failures unresolved. That is neither
qualification of the current Bound/daemon nor an unchanged-suite pass. The
compile-only `d09451d7a2a61e66e13a9ff00233c1def5d841d3` result adds no runtime claim.
