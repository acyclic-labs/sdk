use super::{FileLock, FileLockMode};
#[cfg(unix)]
use crate::is_exclusive_lock_contention;
#[cfg(unix)]
use std::fs::File;
use std::io;
#[cfg(unix)]
use std::os::fd::AsFd as _;
#[cfg(unix)]
use std::path::Path;

#[cfg(unix)]
#[path = "file_lock_test_fixture.rs"]
mod fixture;
#[cfg(unix)]
use fixture::{is_free, open};

/// A held lock of either mode excludes only other lock holders: the file's
/// bytes stay readable and writable through every other handle, on Windows
/// too, where byte-range locks are mandatory for I/O on their range.
/// Control: the whole-file lock `std` takes makes the same read fail.
#[test]
fn a_held_lock_leaves_file_data_readable_and_writable() -> io::Result<()> {
    use std::io::Write as _;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("lock");
    std::fs::write(&path, b"record")?;
    for mode in [FileLockMode::Shared, FileLockMode::Exclusive] {
        let held = FileLock::acquire(open(&path)?, mode)?;
        assert_eq!(std::fs::read(&path)?, b"record");
        OpenOptions::new()
            .write(true)
            .open(&path)?
            .write_all(b"record")?;
        assert!(!is_free(&path)?);
        drop(held);
    }
    assert!(is_free(&path)?);

    let raw = open(&path)?;
    raw.lock()?;
    let read = std::fs::read(&path);
    raw.unlock()?;
    if cfg!(windows) {
        // ERROR_LOCK_VIOLATION: the oracle observes a mandatory range lock.
        assert_eq!(read.map(drop).unwrap_err().raw_os_error(), Some(33));
    } else {
        assert_eq!(read?, b"record");
    }
    Ok(())
}

#[test]
fn a_nonblocking_acquisition_never_waits_for_another_operation() -> io::Result<()> {
    let lock = FileLock::new(tempfile::tempfile()?);
    // An OS operation owns the state mutex until its syscall completes.
    let operation = lock
        .held
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(
        lock.try_lock(FileLockMode::Exclusive).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(operation);
    lock.try_lock(FileLockMode::Exclusive)?;
    assert!(lock.is_held());
    Ok(())
}

#[cfg(unix)]
#[test]
fn a_dropped_lock_is_free_while_a_spawned_process_shares_its_description() -> io::Result<()> {
    let share = |file: &File| -> io::Result<LockHolder> {
        let (control, lifetime) = io::pipe()?;
        Ok(LockHolder {
            child: std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", "file_lock::tests::lock_holder_helper"])
                .env(HOLDER_MODE, "share")
                .stdin(file.try_clone()?)
                .stdout(std::process::Stdio::null())
                .stderr(control)
                .spawn()?,
            lifetime: Some(lifetime),
        })
    };
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("raw-control");

    // Control: a raw lock released by close stays held by the child's copy.
    let raw = open(&path)?;
    FileLock::try_lock_description(raw.as_fd(), FileLockMode::Exclusive)?;
    let sharing = share(&raw)?;
    let other_sharing = share(&raw)?;
    drop(raw);
    assert!(!is_free(&path)?);
    drop(sharing);
    assert!(!is_free(&path)?);
    drop(other_sharing);

    // Other tests can fork while the raw description is open too. Their
    // copies survive until exec, so ending our child is not proof that this
    // raw lock has become free. Exercise explicit release on its own inode.
    let path = directory.path().join("guarded");
    let guarded = FileLock::try_acquire(open(&path)?, FileLockMode::Exclusive)?;
    let _sharing = share(&guarded)?;
    drop(guarded);
    assert!(is_free(&path)?);
    Ok(())
}

#[cfg(unix)]
const HOLDER_MODE: &str = "ACYCLIC_FILE_LOCK_HOLDER_MODE";
#[cfg(unix)]
const HOLDER_PATH: &str = "ACYCLIC_FILE_LOCK_HOLDER_PATH";

#[cfg(unix)]
struct LockHolder {
    child: std::process::Child,
    lifetime: Option<io::PipeWriter>,
}

#[cfg(unix)]
impl Drop for LockHolder {
    fn drop(&mut self) {
        drop(self.lifetime.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Stderr carries a lifetime pipe so stdin can retain the inherited lock.
/// The original parent owns its only writer; losing it terminates this wait.
#[cfg(unix)]
fn wait_for_holder_owner() -> io::Result<()> {
    let mut byte = 0_u8;
    #[allow(
        unsafe_code,
        reason = "waits on the inherited lifetime pipe without duplicating it"
    )]
    // SAFETY: this helper's stderr is the inherited readable lifetime pipe,
    // and the one-byte destination lives until this blocking read returns.
    let count = unsafe { libc::read(libc::STDERR_FILENO, (&raw mut byte).cast(), 1) };
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    Err(if count == 0 {
        io::Error::new(io::ErrorKind::BrokenPipe, "lock holder lifetime owner lost")
    } else {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "lock holder lifetime pipe carried data",
        )
    })
}

/// A process that takes the lock, reports readiness, and lives until killed.
/// "held" keeps the lock until exit; "dropped" releases it through the guard.
#[cfg(unix)]
#[test]
fn lock_holder_helper() -> io::Result<()> {
    let Ok(mode) = std::env::var(HOLDER_MODE) else {
        return Ok(());
    };
    if mode == "share" {
        return wait_for_holder_owner();
    }
    let path = std::path::PathBuf::from(std::env::var_os(HOLDER_PATH).expect("holder path"));
    let file = open(&path)?;
    if mode == "held" {
        FileLock::try_hold_until_exit(file, FileLockMode::Exclusive)?;
    } else {
        drop(FileLock::try_acquire(file, FileLockMode::Exclusive)?);
    }
    acyclic_test_support::readiness::signal(&mut std::io::stdout().lock(), "LOCK_READY")?;
    wait_for_holder_owner()
}

/// Starts a holder in `mode` and returns it once it has taken its lock.
#[cfg(unix)]
fn holder(mode: &str, path: &Path) -> io::Result<LockHolder> {
    let (control, lifetime) = io::pipe()?;
    let child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "file_lock::tests::lock_holder_helper",
            "--nocapture",
        ])
        .env(HOLDER_MODE, mode)
        .env(HOLDER_PATH, path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(control)
        .spawn()?;
    let mut holder = LockHolder {
        child,
        lifetime: Some(lifetime),
    };
    let output = holder
        .child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("holder stdout missing"))?;
    acyclic_test_support::readiness::wait(&mut io::BufReader::new(output), "LOCK_READY")?;
    Ok(holder)
}

/// A lock held until exit stays held for the holder's whole life, after
/// its guard is gone, and its exit alone frees it. Control: a holder that
/// releases through the guard leaves the lock free while it still lives.
#[cfg(unix)]
#[test]
fn a_lock_held_until_exit_is_freed_only_by_the_exit() -> io::Result<()> {
    let directory = tempfile::tempdir()?;

    let control = directory.path().join("dropped");
    let mut released = holder("dropped", &control)?;
    let free_while_alive = is_free(&control)?;
    released.child.kill()?;
    released.child.wait()?;
    assert!(
        free_while_alive,
        "the oracle cannot observe a released lock"
    );

    let path = directory.path().join("held");
    let mut held = holder("held", &path)?;
    let free_while_alive = is_free(&path)?;
    held.child.kill()?;
    held.child.wait()?;
    assert!(
        !free_while_alive,
        "a lock held until exit was released early"
    );
    assert!(is_free(&path)?, "the holder's exit did not free its lock");

    // Losing the original parent endpoint must terminate the real child,
    // including its held-until-exit description, without a timer or signal.
    let path = directory.path().join("owner-lost");
    let mut orphan = holder("held", &path)?;
    assert!(!is_free(&path)?);
    drop(orphan.lifetime.take());
    assert!(!orphan.child.wait()?.success());
    assert!(
        is_free(&path)?,
        "owner loss did not retire the physical holder"
    );
    Ok(())
}
