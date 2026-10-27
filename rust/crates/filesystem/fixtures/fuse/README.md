# Real Linux FUSE packet capture

`linux-kernel-packets.json` contains unedited requests read from `/dev/fuse`
and the replies physically published during
`native_mount::fuse::tests::packets::kernel_packets_replay_through_the_mount_free_entry`.
No request, reply or provider/kernel output was written by hand.

Capture source: signed SDK commit `e5f43e007f7e703609e062e8f09d9e4bccaad616`,
canonical bb2 job `1793061383044-13928-56ca6165`, 2026-10-27 UTC.
The JSON records the kernel version, capture timestamp, test binary and
arguments, and BLAKE3 of the packet-session source. It includes actual INIT,
LOOKUP, READ, WRITE and RELEASE packets.
SHA-256: `25ae9c049004d25b92ea855df2bfcada2d381d738d3d9d31ab5ca4b9a51ec7fb`.

The live capture test passed after mounting, reading, writing, syncing,
releasing and replaying its requests against a fresh source. The enclosing
unfiltered 2,498-test library run reported other failures and hung in
`linux_installed_callbacks_observe_original_finite_mount_cancellation`; its
owner cancelled that job. This artifact is evidence for the successful
capture/replay test, not a pass for the whole library or a virtio-fs guest.

The declared-output runner only copies artifacts from successful jobs, so the
owner retrieved this successful test's existing JSON directly from that job's
source directory over SSH before cancellation. Its bytes were preserved.
The normal unit test `recorded_kernel_packets_replay_without_mounting` replays
this fixture without `/dev/fuse`, credentials, a guest or a mounted directory.

To capture again, from the lane worktree:

```powershell
$env:BUILD_SLOT_REMOTE_FETCH = 'rust/crates/filesystem/fixtures/fuse/linux-kernel-packets.json'
pwsh -NoProfile -File Q:/lanes/bs.ps1 bash -c 'ACYCLIC_FS_FUSE_CAPTURE=/work/src/rust/crates/filesystem/fixtures/fuse/linux-kernel-packets.json cargo test --locked -p acyclic-fs --lib -- --include-ignored --nocapture'
```

Only replace the artifact with actual output from a successful live capture
and replay, and record the new source and job here.
