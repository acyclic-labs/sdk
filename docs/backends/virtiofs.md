# Linux virtio-fs

The Linux transport uses standard vhost-user-fs over a Unix socket. Its two
split virtqueues (hiprio and one request queue) feed the same FUSE packet entry
and projection state as the host `/dev/fuse` mount. It requires shared guest
memory; it does not require DAX or a provider-specific VMM protocol.

This implementation is under construction. The public admitted-session
constructor and `acyclic-virtiofsd` command are not available yet. The commands
below describe the standard frontend attachment to an already running socket;
the validation section distinguishes the two real fixture-source guest boots
from the public admission work that remains.

## Admission and limits

Strict hosted admission is unsupported and must refuse before volume I/O.
Linux can satisfy later private-mapping faults from guest cache without a
server authorization callback. Direct I/O, zero entry/attribute TTLs and no
writeback or `KEEP_CACHE` do not establish the requested revocation boundary.
An explicit standalone `LocalRoot` configuration is a separate authority case;
it cannot substitute for an authenticated hosted parent.

The source admission must retain the original funded source, exact volume and
view, revocable callback guard, and authenticated POSIX identity and metadata
operations. A socket path, guest UID/GID or destination ownership is not that
admission. A public source producer for this complete binding remains pending.

The transport negotiates split rings, indirect descriptors, event indices,
protocol features, and standard CONFIG/MQ/REPLY_ACK. It does not advertise DAX,
notification queues, migration, inflight restoration or device-state transfer.
A transport failure with uncertain completion retires the session; it does not
replay a filesystem mutation. All started physical workers join before their
source or guest-memory mapping is released.

## Standard frontend commands

These examples assume trusted Linux guest kernel and initramfs artifacts,
512 MiB guest RAM, a running server at `/tmp/acyclic-fs.sock`, and mount tag
`acyclic`. The guest kernel must provide `CONFIG_VIRTIO_FS` and its VMM's
virtio transport. The guest initramfs must provide a shell and mount tools.

QEMU uses a shared memory backend and its standard filesystem device:

```sh
qemu-system-x86_64 -enable-kvm -nodefaults -nographic -serial mon:stdio \
  -m 512M -object memory-backend-memfd,id=mem,size=512M,share=on \
  -numa node,memdev=mem -kernel "$GUEST_KERNEL" -initrd "$GUEST_INITRAMFS" \
  -append 'console=ttyS0 rdinit=/bin/sh' \
  -chardev socket,id=fs,path=/tmp/acyclic-fs.sock \
  -device vhost-user-fs-pci,chardev=fs,tag=acyclic
```

cloud-hypervisor uses shared memory and one request queue:

```sh
cloud-hypervisor --kernel "$GUEST_KERNEL" --initramfs "$GUEST_INITRAMFS" \
  --cmdline 'console=ttyS0 rdinit=/bin/sh' --memory size=512M,shared=on \
  --serial tty --console off \
  --fs tag=acyclic,socket=/tmp/acyclic-fs.sock,num_queues=1,queue_size=512
```

Inside either guest, exercise an admitted writable standalone volume:

```sh
mkdir -p /mnt/acyclic
mount -t virtiofs acyclic /mnt/acyclic
printf 'virtio-fs qualification\n' > /mnt/acyclic/qualification
cat /mnt/acyclic/qualification
rm /mnt/acyclic/qualification
test ! -e /mnt/acyclic/qualification
umount /mnt/acyclic
```

The currently inspected upstream Firecracker exposes vhost-user **block**,
and its device/configuration inventory has no filesystem frontend. Its block
configuration cannot attach this socket. A Firecracker build must first expose
the standard virtio filesystem device (device ID 26) and vhost-user-fs frontend;
there is no upstream filesystem attachment command to copy here. The SDK does
not supply a private Firecracker protocol.

Frontend references: [QEMU vhost-user](https://www.qemu.org/docs/master/system/devices/virtio/vhost-user.html),
[cloud-hypervisor virtio-fs](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/docs/fs.md),
[Firecracker vhost-user block](https://github.com/firecracker-microvm/firecracker/blob/main/docs/api_requests/block-vhost-user.md),
[Firecracker device inventory](https://github.com/firecracker-microvm/firecracker/blob/main/src/vmm/src/devices/virtio/mod.rs).
Cache evidence: [Linux v6.18 FUSE file operations](https://github.com/torvalds/linux/blob/v6.18/fs/fuse/file.c).

## Validation

bb2 passed `cargo check --locked -p acyclic-fs --tests` for the shared packet
and socket implementation (job `1793061176450-13144-f5af43b2`). This proves
compilation, not a public source admission, protocol interoperability or mount.

The latest completed Linux filesystem library run, including every existing
ignored mount test, finished in 30.37 seconds: 2,477 passed and 23 failed,
with zero ignored or filtered tests (source `8a24213bc2f9cc07753ef29a7f2def82c0a39059`,
job `1793064983779-30633-ebe95600`). Its compile-only gate passed in 95 seconds;
test compilation took 564 seconds. Worker retirement, event-counter custody,
shared-view isolation, node-facts writes and both kernel packet replays passed.
Original mount cancellation returned ENODEV instead of ECANCELED, and the
cross-descriptor fsync vector failed its second pending-write assertion. There
were also 21 failures routed to the filesystem owner. This is not a passing
suite. The subsequent source preserves the original host source refusal until
physical unmount and shares host/vring readiness; those edits await qualification.

A new real QEMU/cloud-hypervisor vector requires prepared guest artifacts:

```powershell
pwsh -NoProfile -File Q:/lanes/bs.ps1 bash -c 'python3 scripts/prepare-virtiofs-guest.py --output /tmp/acyclic-guest && ACYCLIC_FS_GUEST_ARTIFACTS=/tmp/acyclic-guest cargo test --locked -p acyclic-fs --lib -- --include-ignored --nocapture'
```

The real-kernel capture and replay test passed in that run, as did standard
vhost-user CONFIG negotiation and quiet cancellation/EOF. Its actual INIT,
LOOKUP, READ, WRITE and RELEASE traffic and kernel/binary/source provenance
are retained in [the packet fixture](../../rust/crates/filesystem/fixtures/fuse/README.md).
The mount-free replay of that saved fixture also passed. Both QEMU and
cloud-hypervisor booted real guests, mounted the standard socket, wrote/read
the test bytes, deleted the file and verified absence, unmounted and powered
off with exit status zero. Both servers positively joined with `transport=Ok(())`
and no callback failure. Each server recorded 755 backend reads, 11 backend
writes and 3,255 callback items examined. These are private fixture-source
interoperability results, not public hosted admission or page-fault revocation
proof. The canonical receipt confirms home, targets, outer targets and outer
temporary directory cleanup.

Actual preparation succeeded using the installed
distribution kernel, its actual virtio-fs module and static BusyBox. The helper
verifies the official cloud-hypervisor release asset's SHA-256 and records exact
artifact provenance and copies no host initrd or configuration. The vector
retains the socket and VMM process tree through positive exit/cleanup.
The standard protocol-before-SET_OWNER negotiation, CALL/ERR-before-SET_FEATURES
setup and CALL replacement on a settled enabled queue were exercised by these
actual VMMs. The subsequent guest vector additionally asserts nonzero source
callback work and refuses unexpected transport retirement; its stronger
assertions await execution against that source.
