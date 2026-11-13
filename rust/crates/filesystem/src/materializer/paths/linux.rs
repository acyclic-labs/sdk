use super::*;
use crate::native_host::NativeHostWork as _;
use std::io::Read as _;
use std::os::unix::ffi::OsStrExt as _;

fn mount_id(
    root: &HostRoot,
    work: &NativeMaterializationWork,
) -> Result<u64, NativeTreeMaterializationError> {
    let stat = (&*work).attempt_io(
        WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        },
        || {
            rustix::fs::statx(
                &*root.directory(),
                c"",
                rustix::fs::AtFlags::EMPTY_PATH,
                rustix::fs::StatxFlags::MNT_ID,
            )
            .map_err(std::io::Error::from)
        },
    )?;
    if stat.stx_mask & rustix::fs::StatxFlags::MNT_ID.bits() == 0 {
        return Err(NativeTreeMaterializationError::UnsupportedRename);
    }
    Ok(stat.stx_mnt_id)
}

fn mount_entry(row: &[u8], root: &[u8]) -> Result<(u64, bool), NativeTreeMaterializationError> {
    let mut fields = row.split(|byte| *byte == b' ');
    let identity = fields.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing mount identity")
    })?;
    let identity = std::str::from_utf8(identity)
        .map_err(std::io::Error::other)?
        .parse::<u64>()
        .map_err(std::io::Error::other)?;
    let field = fields.nth(3).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed mountinfo row")
    })?;
    let mut decoded = acyclic_native_runtime::resources::mountinfo_path_bytes(field);
    for byte in root {
        if decoded.next() != Some(*byte) {
            return Ok((identity, false));
        }
    }
    Ok((
        identity,
        if root == b"/" {
            decoded.next().is_some()
        } else {
            decoded.next() == Some(b'/')
        },
    ))
}

/// Device identity cannot distinguish bind mounts. Inspect namespace topology,
/// including mounts nested anywhere below the held checkout, without walking
/// repository contents or following authored links.
pub(super) fn validate_mounts(
    root: &HostRoot,
    operation: &HostRoot,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let identity = mount_id(root, work)?;
    if identity != mount_id(operation, work)? {
        return Err(NativeTreeMaterializationError::MountBoundary);
    }
    let read = WorkCounters {
        backend_read_operations: 1,
        ..WorkCounters::UNCHARGED
    };
    let mut native = &*work;
    let held = native.attempt_io(read, || root.directory().try_clone())?;
    let (path, _path) = crate::native_host::canonical_directory_path_held(held, &mut native)?;
    let mut file = native.attempt_io(read, || std::fs::File::open("/proc/self/mountinfo"))?;
    let mut allocation = work.claim(0, 0, WorkCounters::UNCHARGED)?;
    let mut row = Vec::new();
    let mut witnessed = false;
    let mut chunk = [0_u8; 8192];
    loop {
        work.admit(WorkCounters {
            source_bytes_read: chunk.len() as u64,
            ..read
        })?;
        work.charge(read)?;
        let count = match file.read(&mut chunk) {
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        };
        work.spent(WorkCounters {
            source_bytes_read: count as u64,
            ..WorkCounters::UNCHARGED
        })?;
        if count == 0 {
            // A complete namespace snapshot must contain the held root's
            // mount. Absence or truncation is not a proof of safe topology.
            if !row.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "incomplete mountinfo row",
                )
                .into());
            }
            if !witnessed {
                return Err(NativeTreeMaterializationError::MountBoundary);
            }
            return Ok(());
        }
        for part in chunk[..count].split_inclusive(|byte| *byte == b'\n') {
            let needed = row
                .len()
                .checked_add(part.len())
                .ok_or(crate::WorkError::Overflow)?;
            reserve_buffer(&mut row, needed, &mut allocation)?;
            work.charge(WorkCounters {
                bytes_copied: part.len() as u64,
                items_examined: part.len() as u64,
                ..WorkCounters::UNCHARGED
            })?;
            row.extend_from_slice(part);
            if row.last() == Some(&b'\n') {
                let (mount, below) = mount_entry(&row, path.as_os_str().as_bytes())?;
                witnessed |= mount == identity;
                if below {
                    return Err(NativeTreeMaterializationError::MountBoundary);
                }
                row.clear();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readonly_bind_entries_refuse_layout_before_publication() {
        const CHILD: &str = "ACYCLIC_MOUNT_LAYOUT_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let test = concat!(
                module_path!(),
                "::readonly_bind_entries_refuse_layout_before_publication"
            );
            let test = test.split_once("::").map_or(test, |(_, path)| path);
            let output = std::process::Command::new("unshare")
                .args([
                    "--user",
                    "--map-root-user",
                    "--mount",
                    "--propagation",
                    "private",
                    "--",
                ])
                .arg(std::env::current_exe().unwrap())
                .args(["--exact", test, "--test-threads=1", "--nocapture"])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "namespace child failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("1 passed"),
                "namespace child executed no test"
            );
            return;
        }
        fn command(program: &str, args: &[&std::ffi::OsStr]) {
            let output = std::process::Command::new(program)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{program}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let directory = tempfile::Builder::new()
            .prefix("mount layout with spaces ")
            .tempdir()
            .unwrap();
        let root = directory.path().join("checkout");
        let state = directory.path().join("state");
        let source = directory.path().join("source");
        for path in [&root, &state, &source] {
            std::fs::create_dir(path).unwrap();
        }
        std::fs::write(source.join("file"), b"original").unwrap();
        let stop = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &stop).unwrap();
        validate_native_preparation_location(&root, &state, &work).unwrap();
        let sibling = directory.path().join("checkout-other");
        std::fs::create_dir(&sibling).unwrap();
        command(
            "mount",
            &["--bind".as_ref(), source.as_os_str(), sibling.as_os_str()],
        );
        command(
            "mount",
            &[
                "-o".as_ref(),
                "remount,bind,ro".as_ref(),
                sibling.as_os_str(),
            ],
        );
        // A sibling mount is not below this checkout, even when its path
        // shares the same byte prefix. Its own mount still differs from state.
        let allowed = validate_native_preparation_location(&root, &state, &work);
        let refused = validate_native_preparation_location(&sibling, &state, &work);
        command("umount", &[sibling.as_os_str()]);
        allowed.unwrap();
        assert!(
            matches!(refused, Err(NativeTreeMaterializationError::MountBoundary)),
            "{refused:?}"
        );
        // A nested bind must be found from mount topology too: never recursively
        // scan source content, or only compare the two directory device ids.
        for relative in ["entry", "nested/entry"] {
            let entry = root.join(relative);
            std::fs::create_dir_all(&entry).unwrap();
            command(
                "mount",
                &["--bind".as_ref(), source.as_os_str(), entry.as_os_str()],
            );
            command(
                "mount",
                &["-o".as_ref(), "remount,bind,ro".as_ref(), entry.as_os_str()],
            );
            use std::os::unix::fs::MetadataExt as _;
            assert_eq!(
                std::fs::metadata(&entry).unwrap().dev(),
                std::fs::metadata(&state).unwrap().dev()
            );
            let refusal = validate_native_preparation_location(&root, &state, &work);
            let crossed = std::fs::rename(entry.join("file"), state.join("moved"));
            command("umount", &[entry.as_os_str()]);
            assert!(
                matches!(refusal, Err(NativeTreeMaterializationError::MountBoundary)),
                "{refusal:?}"
            );
            assert_eq!(crossed.unwrap_err().raw_os_error(), Some(libc::EXDEV));
            assert_eq!(std::fs::read(source.join("file")).unwrap(), b"original");
            assert!(!state.join("moved").exists());
        }
        validate_native_preparation_location(&root, &state, &work).unwrap();
    }
}
