use acyclic_native_runtime::{FileLock, FileLockMode, is_exclusive_lock_contention};
use std::io;

#[path = "../../src/file_lock_test_fixture.rs"]
mod fixture;
use fixture::{is_free, open};

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
    drop(exclusive);
    assert!(is_free(&path)?);
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
#[test]
fn a_conflicting_lock_wait_completes_on_release_or_owner_drop() -> io::Result<()> {
    for explicit in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("lock");
        let owner = FileLock::acquire(open(&path)?, FileLockMode::Exclusive)?;
        let waiter = FileLock::new(open(&path)?);
        let (entered, observed) = std::sync::mpsc::channel();
        let (completed, completion) = std::sync::mpsc::channel();
        let waiting = std::thread::spawn(move || -> io::Result<()> {
            // Real contention is the negative control for the wait's safety.
            assert_eq!(
                waiter.try_lock(FileLockMode::Exclusive).unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
            entered.send(()).map_err(io::Error::other)?;
            waiter.lock(FileLockMode::Exclusive)?;
            completed.send(()).map_err(io::Error::other)
        });
        observed.recv().map_err(io::Error::other)?;
        assert_eq!(
            completion.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        );
        if explicit {
            owner.unlock()?;
        }
        drop(owner);
        completion.recv().map_err(io::Error::other)?;
        waiting
            .join()
            .map_err(|_| io::Error::other("lock waiter panicked"))??;
    }
    Ok(())
}
