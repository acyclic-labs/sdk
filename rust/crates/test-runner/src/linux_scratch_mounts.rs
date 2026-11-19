//! Detach Acyclic FUSE mounts left in a case's closed scratch directory.

use std::ffi::OsString;
use std::io;
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

struct Mount {
    point: PathBuf,
    owned: bool,
}

fn mounts_in(table: &[u8], scratch: &Path, user: u32) -> Vec<Mount> {
    use acyclic_process_resources::resources::mountinfo_path_bytes;
    let owner = format!("user_id={user}");
    table
        .split(|byte| *byte == b'\n')
        .filter_map(|line| {
            let mut fields = line.split(|byte| *byte == b' ');
            let point = PathBuf::from(OsString::from_vec(
                mountinfo_path_bytes(fields.nth(4)?).collect(),
            ));
            if !point.starts_with(scratch) {
                return None;
            }
            let mut filesystem = fields.skip_while(|field| *field != b"-").skip(1);
            let kind = filesystem.next()?;
            let source = filesystem.next()?;
            let options = filesystem.next()?;
            let owned = matches!(kind, b"fuse" | b"fuse.acyclic-fs")
                && source == b"acyclic-fs"
                && options
                    .split(|byte| *byte == b',')
                    .any(|option| option == owner.as_bytes());
            Some(Mount { point, owned })
        })
        .collect()
}

fn owned_roots(mounts: &[Mount]) -> Vec<&Path> {
    mounts
        .iter()
        .filter(|mount| mount.owned)
        .filter(|mount| {
            !mounts.iter().any(|other| {
                if std::ptr::eq(*mount, other) {
                    return false;
                }
                // The outer detach already removes its children, with no
                // lookup through a disconnected FUSE directory. A foreign
                // descendant or an ambiguous stack prevents that detach.
                mount.point.starts_with(&other.point)
                    || (!other.owned && other.point.starts_with(&mount.point))
            })
        })
        .map(|mount| mount.point.as_path())
        .collect()
}

/// The caller has joined its case or holds an abandoned runner's directory lock.
/// Identify the mount without statting a disconnected FUSE endpoint.
pub(super) fn detach(scratch: &Path) -> io::Result<bool> {
    let table = std::fs::read("/proc/self/mountinfo")?;
    let mounts = mounts_in(
        &table,
        scratch,
        acyclic_native_runtime::private_file::effective_user(),
    );
    let points = owned_roots(&mounts);
    for point in &points {
        detach_one(point)?;
    }
    Ok(!points.is_empty())
}

fn detach_one(point: &Path) -> io::Result<()> {
    let helper = acyclic_native_runtime::FUSE_MOUNT_HELPERS
        .iter()
        .map(|program| std::ffi::OsStr::from_bytes(program.to_bytes()))
        .find(|program| Path::new(program).is_file());
    let (program, flags) = match helper {
        Some(helper) => (helper, ["-u", "-q", "-z", "--"]),
        None => (
            std::ffi::OsStr::from_bytes(c"/bin/umount".to_bytes()),
            ["-i", "-l", "--no-canonicalize", "--"],
        ),
    };
    let status = Command::new(program)
        .args(flags)
        .arg(point)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "unmount {}: {status}",
            point.display()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_cleanup_selects_only_owned_unambiguous_mounts() {
        let table = b"\
1 0 0:1 / /tmp rw - tmpfs tmpfs rw\n\
2 1 0:2 / /tmp/case/with\\040space rw - fuse acyclic-fs rw,user_id=42\n\
3 2 0:3 / /tmp/case/with\\040space/child rw - fuse acyclic-fs rw,user_id=42\n\
4 1 0:4 / /tmp/case/blocked rw - fuse acyclic-fs rw,user_id=42\n\
5 4 0:5 / /tmp/case/blocked/foreign rw - fuse foreign rw,user_id=42\n\
6 1 0:6 / /tmp/case/different-user rw - fuse acyclic-fs rw,user_id=420\n\
7 1 0:7 / /tmp/case-next/foreign rw - fuse acyclic-fs rw,user_id=42\n\
8 1 0:8 / /tmp/case/stack rw - fuse acyclic-fs rw,user_id=42\n\
9 8 0:9 / /tmp/case/stack rw - fuse acyclic-fs rw,user_id=42\n";
        let mounts = mounts_in(table, Path::new("/tmp/case"), 42);
        assert_eq!(owned_roots(&mounts), [Path::new("/tmp/case/with space")]);
    }
}
