use super::{FileLock, FileLockMode};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

fn open(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
}

fn is_free(path: &Path) -> io::Result<bool> {
    match FileLock::try_acquire(open(path)?, FileLockMode::Exclusive) {
        Ok(_probe) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
        Err(error) => Err(error),
    }
}

#[test]
fn modes_exclude_exactly_their_conflicts() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("lock");
    let first = FileLock::try_acquire(open(&path)?, FileLockMode::Shared)?;
    let second = FileLock::try_acquire(open(&path)?, FileLockMode::Shared)?;
    assert!(!is_free(&path)?);
    drop(first);
    assert!(!is_free(&path)?);
    drop(second);
    assert!(is_free(&path)?);

    let exclusive = FileLock::try_acquire(open(&path)?, FileLockMode::Exclusive)?;
    let shared = FileLock::try_acquire(open(&path)?, FileLockMode::Shared);
    assert_eq!(
        shared.map(|_| ()).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        exclusive
            .try_lock(FileLockMode::Exclusive)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    exclusive.unlock()?;
    assert!(!exclusive.is_held());
    assert!(is_free(&path)?);
    exclusive.lock(FileLockMode::Exclusive)?;
    assert!(!is_free(&path)?);
    Ok(())
}

#[test]
fn a_nonblocking_acquisition_never_waits_for_another_operation() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("lock");
    let lock = FileLock::new(open(&path)?);
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

/// An explicit release ends this handle's custody only: a lock retaken
/// through another handle on the same description survives its drop.
#[test]
fn a_released_handle_never_unlocks_a_later_holder_of_its_description() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("lock");
    let first = FileLock::try_acquire(open(&path)?, FileLockMode::Exclusive)?;
    let same_description = first.try_clone()?;
    first.unlock()?;
    let second = FileLock::try_acquire(same_description, FileLockMode::Exclusive)?;
    drop(first);
    assert!(!is_free(&path)?);
    drop(second);
    assert!(is_free(&path)?);
    Ok(())
}

/// A process spawned while a lock is held keeps a copy of its description.
/// Dropping the guard still frees the lock; closing a raw lock does not.
#[cfg(unix)]
#[test]
fn a_dropped_lock_is_free_while_a_spawned_process_shares_its_description() -> io::Result<()> {
    struct Sharing(std::process::Child);
    impl Drop for Sharing {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let share = |file: &File| -> io::Result<Sharing> {
        Ok(Sharing(
            std::process::Command::new("sleep")
                .arg("60")
                .stdin(file.try_clone()?)
                .spawn()?,
        ))
    };
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("raw-control");

    // Control: a raw lock released by close stays held by the child's copy.
    let raw = open(&path)?;
    raw.try_lock().map_err(io::Error::from)?;
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

/// A process that takes the lock, reports readiness, and lives until killed.
/// "held" keeps the lock until exit; "dropped" releases it through the guard.
#[cfg(unix)]
#[test]
fn lock_holder_helper() -> io::Result<()> {
    let Ok(mode) = std::env::var(HOLDER_MODE) else {
        return Ok(());
    };
    let path = std::path::PathBuf::from(std::env::var_os(HOLDER_PATH).expect("holder path"));
    let file = open(&path)?;
    if mode == "held" {
        FileLock::try_hold_until_exit(file, FileLockMode::Exclusive)?;
    } else {
        drop(FileLock::try_acquire(file, FileLockMode::Exclusive)?);
    }
    std::fs::write(path.with_extension("ready"), b"ready")?;
    loop {
        std::thread::park();
    }
}

/// Starts a holder in `mode` and returns it once it has taken its lock.
#[cfg(unix)]
fn holder(mode: &str, path: &Path) -> io::Result<std::process::Child> {
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "file_lock::tests::lock_holder_helper",
            "--nocapture",
        ])
        .env(HOLDER_MODE, mode)
        .env(HOLDER_PATH, path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    while !path.with_extension("ready").exists() {
        assert!(child.try_wait()?.is_none(), "the holder exited early");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    Ok(child)
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
    released.kill()?;
    released.wait()?;
    assert!(
        free_while_alive,
        "the oracle cannot observe a released lock"
    );

    let path = directory.path().join("held");
    let mut held = holder("held", &path)?;
    let free_while_alive = is_free(&path)?;
    held.kill()?;
    held.wait()?;
    assert!(
        !free_while_alive,
        "a lock held until exit was released early"
    );
    assert!(is_free(&path)?, "the holder's exit did not free its lock");
    Ok(())
}
