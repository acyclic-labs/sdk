# Native mount qualification

The native-mount gate executes the same real-kernel behavioral matrix against
Linux FUSE, macOS loopback NFS, and Windows ProjFS. It covers projected reads,
create/write/read, nested paths, atomic replacement, rename, deletion,
concurrent handles, host watchers, orderly restoration, crash recovery, hard
links, symbolic links/reparse points, metadata persistence, platform case
behavior, bounded escape probes, and before/after fingerprints of content,
object types, link targets, and stable platform stat metadata in both the source
checkout and its Git administrative directory. Git HEAD and porcelain status
are compared independently.

The macOS loopback NFS mount does not emit FSEvents, so that backend exercises
the supported polling watcher. Linux and Windows exercise their native watcher.

Every run writes an `acyclic-native-mount-qualification-v1` JSON receipt. A
developer may use `--allow-unsupported` when invoking the binary directly; that
produces a successful `skipped` case with the capability probe's exact reason.
The dedicated CI scripts always pass `--require-kind`, so a missing or different
backend fails rather than skipping.

The self-hosted mount runners execute only trusted `main` pushes or explicit
manual dispatches; pull-request code is never run automatically on privileged,
persistent mount hosts.

## Dedicated runner prerequisites

- `linux-fuse`: Linux x86-64, Rust 1.94, a readable/writable `/dev/fuse`, and
  `fusermount3`. The runner service account must be permitted to create FUSE
  mounts. Labels: `self-hosted,native-mount,linux-x64,fuse`.
- `macos-nfs`: Apple silicon macOS, Rust 1.94, `/sbin/mount_nfs`, `/sbin/umount`,
  and permission for the service account to mount the in-process loopback NFS
  export. Labels: `self-hosted,native-mount,macos-arm64,loopback-nfs`.
- `windows-projfs`: Windows x86-64, Rust 1.94, Developer Mode or an equivalent
  symbolic-link privilege, and the `Client-ProjFS` optional feature enabled.
  Labels: `self-hosted,native-mount,windows-x64,projfs`.

Run locally from the repository root:

```text
bash scripts/qualify-native-mount.sh linux-fuse
bash scripts/qualify-native-mount.sh macos-nfs
powershell -File scripts/qualify-native-mount.ps1 -Backend windows-projfs
```

## Projection fix proven by the matrix

The Windows qualification exposed that orderly `PrjStopVirtualizing` leaves a
hydrated ProjFS reparse tree at the caller's mount destination. That residue is
not an ordinary empty directory and prevents reliable reuse. `ProjFsSession`
now authenticates and removes only that stopped ProjFS cache using the existing
bounded recovery path, recreates the caller's empty destination, and retains
cleanup state when cleanup fails so an exact stop retry can finish it. No
checkout, publication, or distributed-filesystem semantics changed.

The Windows metadata case also proved that ProjFS reports a metadata-only
`FileBasicInfo` change as a handle close without data modification. That close
previously skipped host capture when the path already existed, discarding such
changes. The callback now records per-open basic-metadata baselines and captures
only representable changes. It ignores ProjFS's expected placeholder-hydration
attribute transition and automatic access-time updates, so ordinary reads stay
side-effect free while the existing bounded capture transaction preserves
authored attributes and timestamps.

The macOS crash case also proved that comparing `st_dev` with the destination's
parent is not a valid NFS mount-presence test: the loopback export can report the
same device identity while still mounted. Recovery now checks Darwin's mount
table by exact mount-point name before removing the destination, preventing the
observed `EBUSY` race without changing NFS request handling. The crash test
allows one exact recovery retry because a forced detach of a dead loopback NFS
server may finish asynchronously after the bounded unmount helper returns.
