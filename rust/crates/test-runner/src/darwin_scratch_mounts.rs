//! Retire the NFS mounts this user left in a case's closed scratch directory, through the shared
//! Disk Arbitration retirement: macOS refuses a non-root `unmount(2)` even of the user's own mount.

use acyclic_process_resources::resources::Mount;
use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;

/// This user's NFS mounts at or beneath `scratch`, innermost first, so no retirement waits on a
/// mount another one still covers. Every Darwin transport the SDK mounts is an NFS mount.
fn owned_innermost_first<'a>(mounts: &'a [Mount], scratch: &Path, user: u32) -> Vec<&'a Path> {
    let mut points: Vec<&Path> = mounts
        .iter()
        .filter(|mount| {
            mount.owner == Some(user)
                && mount.kind.as_deref() == Some("nfs")
                && mount.point.starts_with(scratch)
        })
        .map(|mount| mount.point.as_path())
        .collect();
    points.sort_by_key(|point| std::cmp::Reverse(point.components().count()));
    points
}

/// The caller has joined its case or holds an abandoned runner's directory lock, and the runner's
/// directory is owner-only, so each of this user's mounts beneath `scratch` is that case's own.
/// Reads the kernel's mount table only: a dead NFS root is never opened.
pub(super) fn detach(scratch: &Path) -> io::Result<bool> {
    let mounts = acyclic_process_resources::resources::mount_points()?;
    let points = owned_innermost_first(
        &mounts,
        scratch,
        acyclic_native_runtime::private_file::effective_user(),
    );
    for point in &points {
        acyclic_darwin_mount_retire::retire(&CString::new(point.as_os_str().as_bytes())?)?;
    }
    Ok(!points.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn scratch_cleanup_selects_only_owned_nfs_mounts_innermost_first() {
        let mount = |point: &str, owner: u32, kind: &str| Mount {
            point: PathBuf::from(point),
            owner: Some(owner),
            kind: Some(kind.to_owned()),
        };
        let mounts = [
            mount("/tmp", 0, "apfs"),
            mount("/tmp/case/owned", 42, "nfs"),
            mount("/tmp/case/owned/inner", 42, "nfs"),
            mount("/tmp/case/image", 42, "apfs"),
            mount("/tmp/case/other-user", 420, "nfs"),
            mount("/tmp/case-next/owned", 42, "nfs"),
        ];
        assert_eq!(
            owned_innermost_first(&mounts, Path::new("/tmp/case"), 42),
            [
                Path::new("/tmp/case/owned/inner"),
                Path::new("/tmp/case/owned")
            ]
        );
    }
}
