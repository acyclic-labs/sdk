use super::*;
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

const CHILD: &str = "ACYCLIC_CLAIM_CHILD";
/// Printed once the child holds its claim. libtest prints it after the
/// test name on the same line.
const HELD: &str = "acyclic-claim-held";

fn contended(result: io::Result<StoreClaim>) -> bool {
    matches!(result, Err(error) if is_exclusive_lock_contention(&error))
}

#[test]
fn a_claim_excludes_a_second_claim_until_it_is_released() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let claim = StoreClaim::acquire(&path)?;
    assert!(contended(StoreClaim::acquire(&path)));
    // Another name for the same file is the same claim.
    let alias = directory.path().join(".").join("owner.lock");
    assert!(contended(StoreClaim::acquire(&alias)));
    drop(claim);
    assert_eq!(
        std::fs::metadata(&path)?.len(),
        0,
        "release clears the record"
    );
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

#[test]
fn racing_claims_admit_exactly_one_owner() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = Arc::new(directory.path().join("owner.lock"));
    for _ in 0..32 {
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let claims = (0..8)
            .map(|_| {
                let (path, barrier) = (Arc::clone(&path), Arc::clone(&barrier));
                std::thread::spawn(move || {
                    barrier.wait();
                    StoreClaim::acquire(&path)
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|thread| {
                thread
                    .join()
                    .map_err(|_| io::Error::other("claimant panicked"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut owners = 0;
        for claim in claims {
            match claim {
                Ok(_) => owners += 1,
                Err(error) if is_exclusive_lock_contention(&error) => {}
                Err(error) => return Err(error),
            }
        }
        assert_eq!(owners, 1);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn held_claims_keep_no_descriptor_open() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    // Other tests open descriptors concurrently, so count only this test's.
    let open = || -> io::Result<usize> {
        let mut count = 0;
        for entry in std::fs::read_dir("/proc/self/fd")? {
            if std::fs::read_link(entry?.path())
                .is_ok_and(|target| target.starts_with(directory.path()))
            {
                count += 1;
            }
        }
        Ok(count)
    };
    let claims = (0..256)
        .map(|index| StoreClaim::acquire(&directory.path().join(format!("{index}.lock"))))
        .collect::<io::Result<Vec<_>>>()?;
    assert_eq!(open()?, 0, "{} claims", claims.len());
    drop(claims);
    Ok(())
}

/// A claim held by another process, which keeps it until its input closes.
struct Owner(Child);

impl Owner {
    fn spawn(path: &Path) -> io::Result<Self> {
        Self::spawn_with(path, &[])
    }

    fn spawn_with(path: &Path, environment: &[(&str, &Path)]) -> io::Result<Self> {
        let test = concat!(module_path!(), "::claim_owner_process");
        let test = test.split_once("::").map_or(test, |(_, path)| path);
        let mut child = Command::new(std::env::current_exe()?)
            .args(["--exact", test, "--test-threads=1", "--nocapture"])
            .env(CHILD, path)
            .envs(environment.iter().copied())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("no stdout"))?;
        let mut line = String::new();
        let mut lines = BufReader::new(stdout);
        while !line.contains(HELD) {
            line.clear();
            if lines.read_line(&mut line)? == 0 {
                return Err(io::Error::other("owner process exited before claiming"));
            }
        }
        // Keep reading so the child's later output never meets a closed pipe.
        std::thread::spawn(move || io::copy(&mut lines, &mut io::sink()));
        Ok(Self(child))
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn claim_owner_process() -> io::Result<()> {
    let Some(path) = std::env::var_os(CHILD) else {
        return Ok(());
    };
    let _claim = StoreClaim::acquire(Path::new(&path))?;
    println!("{HELD}");
    io::stdout().flush()?;
    // Hold the claim until the parent closes this input or kills this process.
    io::stdin().read_to_end(&mut Vec::new())?;
    Ok(())
}

/// The process lock a claim file's record names.
fn recorded_process_lock(path: &Path) -> io::Result<PathBuf> {
    let record = std::fs::read(path)?;
    crate::claim::recorded_process_lock(&record)
        .ok_or_else(|| io::Error::other(format!("no record in {record:?}")))
}

#[test]
fn another_process_claim_excludes_until_that_process_dies() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let mut owner = Owner::spawn(&path)?;
    assert!(contended(StoreClaim::acquire(&path)));
    owner.0.kill()?;
    owner.0.wait()?;
    // The dead owner's record remains, but its process lock lapsed with it.
    // Another process's sweep may already have removed the lock itself.
    recorded_process_lock(&path)?;
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

/// A process-lock probe that fails other than by contention, as an OS lock
/// does on a descriptor without the access it needs, or on an I/O error.
fn failing_probe(_: std::fs::File) -> io::Result<crate::FileLock> {
    Err(io::Error::from(io::ErrorKind::PermissionDenied))
}

#[test]
fn an_owner_whose_lock_probe_fails_keeps_its_claim() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let owner = Owner::spawn(&path)?;
    // The probe never answered, so nothing proves the owner dead.
    assert!(contended(StoreClaim::acquire_probing(&path, failing_probe)));
    let file = crate::FileLock::new(std::fs::File::open(&path)?);
    let check = StoreClaim::check_probing(&file, failing_probe)?;
    assert!(check.claimed, "an unanswered probe admitted: {check:?}");
    drop(file);
    // The same owner, probed for real, is alive; once gone it is dead.
    assert!(contended(StoreClaim::acquire(&path)));
    drop(owner);
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

#[test]
fn another_process_claim_survives_removal_of_its_process_lock() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let owner = Owner::spawn(&path)?;
    let lock = recorded_process_lock(&path)?;
    match std::fs::remove_file(&lock) {
        // A temporary-file cleaner can remove a live process's lock on Unix.
        Ok(()) => assert!(contended(StoreClaim::acquire(&path))),
        // Windows denies removing it while its owner lives.
        Err(error) if cfg!(windows) => {
            assert!(contended(StoreClaim::acquire(&path)), "{error}");
        }
        Err(error) => return Err(error),
    }
    drop(owner);
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

#[test]
fn a_released_claim_admits_another_process() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let mut owner = Owner::spawn(&path)?;
    // Closing its input ends the owner normally, releasing the claim.
    drop(owner.0.stdin.take());
    assert!(owner.0.wait()?.success());
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

#[test]
fn a_torn_record_names_no_owner() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let claim = StoreClaim::acquire(&path)?;
    let record = std::fs::read(&path)?;
    drop(claim);
    // A record cut anywhere, as by its writer's death mid-write.
    for cut in [1, record.len() / 2, record.len() - 1] {
        std::fs::write(&path, record.get(..cut).unwrap_or_default())?;
        drop(StoreClaim::acquire(&path)?);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn a_runtime_directory_with_newlines_names_its_owner_exactly() -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir()?;
    let runtime = directory.path().join("run\nacyclic-claim-v1\n1\n\n");
    std::fs::create_dir(&runtime)?;
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))?;
    let path = directory.path().join("owner.lock");
    let mut owner = Owner::spawn_with(&path, &[("XDG_RUNTIME_DIR", &runtime)])?;
    let lock = recorded_process_lock(&path)?;
    assert!(lock.starts_with(&runtime), "{lock:?}");
    assert!(lock.exists());
    assert!(contended(StoreClaim::acquire(&path)));
    owner.0.kill()?;
    owner.0.wait()?;
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

/// A dead owner that its parent has not yet waited for is a zombie, which
/// holds no files, so it owns nothing, even once a sweep has removed its
/// process lock and only its pid is left to probe.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[test]
fn an_owner_that_died_unwaited_owns_nothing() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let mut owner = Owner::spawn(&path)?;
    let lock = recorded_process_lock(&path)?;
    owner.0.kill()?;
    exited_unwaited(owner.0.id())?;
    // As another process's sweep does, once the dead owner's lock is free.
    let held =
        crate::FileLock::try_acquire(std::fs::File::open(&lock)?, crate::FileLockMode::Shared)?;
    std::fs::remove_file(&lock)?;
    drop(held);
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

/// Blocks until child `pid` exits, leaving it a zombie: std waits only to reap.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[allow(unsafe_code, reason = "FFI: waitid without reaping")]
fn exited_unwaited(pid: u32) -> io::Result<()> {
    // SAFETY: siginfo_t is plain data, valid when zeroed.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    loop {
        // SAFETY: waitid writes only into `info`, which outlives the call.
        let waited = unsafe {
            libc::waitid(
                libc::P_PID,
                libc::id_t::from(pid),
                &raw mut info,
                libc::WEXITED | libc::WNOWAIT,
            )
        };
        if waited == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// A record naming process `pid` by `start`, whose process lock is gone.
#[cfg(target_os = "linux")]
fn lockless_record(directory: &Path, pid: u32, start: &[u8]) -> Vec<u8> {
    crate::claim::foreign_record(pid, start, &directory.join("gone.lock"))
}

#[cfg(target_os = "linux")]
#[test]
fn an_owner_whose_death_is_unproven_refuses() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    // No start identity was recorded, so no probe can tell this pid's
    // process from the owner: unknown, which refuses.
    std::fs::write(&path, lockless_record(directory.path(), u32::MAX, b""))?;
    assert!(contended(StoreClaim::acquire(&path)));
    // A start identity of another boot proves the owner died.
    std::fs::write(
        &path,
        lockless_record(directory.path(), std::process::id(), b"another-boot:1"),
    )?;
    drop(StoreClaim::acquire(&path)?);
    // A process that does not exist under a known boot is dead too.
    std::fs::write(
        &path,
        lockless_record(directory.path(), u32::MAX, b"boot:1"),
    )?;
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

#[test]
fn release_never_clears_a_record_in_a_file_it_did_not_claim() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let moved = directory.path().join("moved.lock");
    let first = StoreClaim::acquire(&path)?;
    // The claimed file moves away and another store claims a new file there.
    std::fs::rename(&path, &moved)?;
    let second = StoreClaim::acquire(&path)?;
    let recorded = std::fs::read(&path)?;
    drop(first);
    assert_eq!(
        std::fs::read(&path)?,
        recorded,
        "the second claim's record survives"
    );
    drop(second);
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    Ok(())
}

/// `path` relative to this process's working directory.
#[cfg(unix)]
fn relative_to_current(path: &Path) -> io::Result<PathBuf> {
    let current = std::env::current_dir()?;
    let mut relative = PathBuf::new();
    for _ in current.components().skip(1) {
        relative.push("..");
    }
    relative.push(path.strip_prefix("/").map_err(io::Error::other)?);
    Ok(relative)
}

#[cfg(unix)]
#[test]
fn a_claim_by_relative_path_is_held_by_its_absolute_path() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let relative = relative_to_current(&path)?;
    assert!(relative.is_relative());
    let claim = StoreClaim::acquire(&relative)?;
    // Release reopens this path, which must not depend on the working
    // directory at release.
    assert_eq!(claim.path(), std::path::absolute(&relative)?);
    assert!(contended(StoreClaim::acquire(&path)));
    drop(claim);
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    Ok(())
}

/// A runtime directory whose process lock path makes a record longer than
/// any record: `depth` nested directories of 200 bytes.
#[cfg(target_os = "linux")]
fn long_runtime_directory(base: &Path, depth: usize) -> io::Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt as _;
    let mut runtime = base.to_path_buf();
    for _ in 0..depth {
        runtime.push("r".repeat(200));
    }
    std::fs::create_dir_all(&runtime)?;
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))?;
    Ok(runtime)
}

#[cfg(target_os = "linux")]
#[test]
fn a_process_whose_record_would_not_fit_holds_its_claims_instead() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let runtime = long_runtime_directory(directory.path(), 20)?;
    let path = directory.path().join("owner.lock");
    let mut owner = Owner::spawn_with(&path, &[("XDG_RUNTIME_DIR", &runtime)])?;
    // No record that a reader could take only in part.
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    assert!(contended(StoreClaim::acquire(&path)));
    owner.0.kill()?;
    owner.0.wait()?;
    drop(StoreClaim::acquire(&path)?);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn a_claim_file_longer_than_any_record_refuses() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    // A whole record of a dead owner, and then more: not that record.
    let mut bytes = lockless_record(directory.path(), u32::MAX, b"boot:1");
    std::fs::write(&path, &bytes)?;
    drop(StoreClaim::acquire(&path)?);
    bytes.resize(crate::claim::RECORD_LIMIT + 1, 0);
    std::fs::write(&path, &bytes)?;
    assert!(contended(StoreClaim::acquire(&path)));
    let file = crate::FileLock::new(std::fs::File::open(&path)?);
    assert!(StoreClaim::is_claimed(&file)?);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn a_claim_check_counts_each_read_it_makes() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let checked =
        |path: &Path| StoreClaim::check(&crate::FileLock::new(std::fs::File::open(path)?));
    std::fs::write(&path, b"")?;
    assert_eq!(checked(&path)?.read_operations, 1, "the record alone");
    let claim = StoreClaim::acquire(&path)?;
    assert_eq!(
        checked(&path)?,
        crate::ClaimCheck {
            claimed: true,
            read_operations: 1
        },
        "this process's own record needs no probe"
    );
    drop(claim);
    // A dead owner whose process lock is gone: the lock, the boot, the process.
    std::fs::write(
        &path,
        lockless_record(directory.path(), u32::MAX, b"boot:1"),
    )?;
    assert_eq!(
        checked(&path)?.read_operations,
        StoreClaim::CHECK_READ_OPERATIONS
    );
    Ok(())
}

/// A record naming a process whose lock path is as long as a record allows,
/// so a check makes every allocation and probe it can.
#[cfg(target_os = "linux")]
fn longest_lockless_record(directory: &Path) -> Vec<u8> {
    let mut lock = directory.join("gone");
    let mut record = Vec::new();
    loop {
        let longer = lock.join("l");
        let candidate =
            crate::claim::foreign_record(std::process::id(), b"another-boot:1", &longer);
        if candidate.len() > crate::claim::RECORD_LIMIT {
            return record;
        }
        lock = longer;
        record = candidate;
    }
}

#[cfg(target_os = "linux")]
#[test]
fn a_claim_check_allocates_within_its_bound() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("owner.lock");
    let held = directory.path().join("held.lock");
    let _claim = StoreClaim::acquire(&held)?;
    let mut oversized = lockless_record(directory.path(), u32::MAX, b"boot:1");
    oversized.resize(crate::claim::RECORD_LIMIT + 1, 0);
    let records = [
        Vec::new(),
        longest_lockless_record(directory.path()),
        oversized,
        std::fs::read(&held)?,
    ];
    for record in records {
        std::fs::write(&path, &record)?;
        let file = crate::FileLock::new(std::fs::File::open(&path)?);
        let mut checked = None;
        let allocated = acyclic_test_support::measure(|| {
            checked = Some(StoreClaim::check(&file).map(|check| check.read_operations));
        });
        let read_operations = checked.ok_or_else(|| io::Error::other("unchecked"))??;
        assert!(
            allocated.count_total <= StoreClaim::CHECK_ALLOCATIONS
                && allocated.bytes_total <= StoreClaim::CHECK_ALLOCATION_BYTES,
            "a {}-byte record checked in {read_operations} reads allocated {allocated:?}",
            record.len()
        );
    }
    Ok(())
}

const CHECK_CHILD: &str = "ACYCLIC_CLAIM_CHECK_CHILD";

#[test]
fn claim_check_process() -> io::Result<()> {
    let Some(path) = std::env::var_os(CHECK_CHILD) else {
        return Ok(());
    };
    StoreClaim::check(&crate::FileLock::new(std::fs::File::open(path)?)).map(drop)
}

/// A check reads another store's claim; it never establishes this
/// process's own lock, which would create, write and sweep files unmetered.
#[cfg(target_os = "linux")]
#[test]
fn a_claim_check_establishes_no_process_lock() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime)?;
    let path = directory.path().join("owner.lock");
    std::fs::write(
        &path,
        lockless_record(directory.path(), u32::MAX, b"boot:1"),
    )?;
    let test = concat!(module_path!(), "::claim_check_process");
    let test = test.split_once("::").map_or(test, |(_, path)| path);
    let status = Command::new(std::env::current_exe()?)
        .args(["--exact", test, "--test-threads=1"])
        .env(CHECK_CHILD, &path)
        .env("XDG_RUNTIME_DIR", &runtime)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    assert!(status.success());
    assert_eq!(std::fs::read_dir(&runtime)?.count(), 0);
    Ok(())
}

/// A store is removed while its claim lives, and a new store's claim file
/// reuses the removed file's identity, as XFS and ext4 reuse inodes at once.
/// The new store is claimable, and stays claimed after the stale claim drops.
#[cfg(unix)]
#[test]
fn a_new_file_reusing_a_removed_claim_files_identity_is_claimable() -> io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let directory = tempfile::tempdir()?;
    let removed = directory.path().join("removed.lock");
    let stale = StoreClaim::acquire(&removed)?;
    let identity = std::fs::metadata(&removed)?.ino();
    std::fs::remove_file(&removed)?;
    // Filesystems that never reuse inodes, such as tmpfs, cannot show this.
    let Some(reused) = (0..256).find_map(|index| {
        let path = directory.path().join(format!("{index}.lock"));
        std::fs::File::create(&path).ok()?;
        (std::fs::metadata(&path).ok()?.ino() == identity).then_some(path)
    }) else {
        return Ok(());
    };
    let fresh = StoreClaim::acquire(&reused)?;
    drop(stale);
    assert!(contended(StoreClaim::acquire(&reused)));
    drop(fresh);
    drop(StoreClaim::acquire(&reused)?);
    Ok(())
}
