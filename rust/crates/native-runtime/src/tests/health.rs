use super::*;

#[cfg(windows)]
#[test]
fn healthy_registry_does_not_retain_exclusive_handle() -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;

    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("exclusive-native-file");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .share_mode(0)
        .open(&path)?;
    let native = NativeFile::from_file(file, None).map_err(|(error, _)| error)?;
    drop(native);
    // No unrelated admission or registry sweep is required to release
    // the caller's exclusive Windows share lock.
    File::open(&path)?;
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn independently_opened_aliases_share_fencing_and_uncertainty() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("identity");
    let alias = temporary.path().join("hard-link");
    std::fs::write(&path, [])?;
    let first = bounded_write_file(&path, true)?;
    std::fs::hard_link(&path, &alias)?;
    let second = bounded_write_file(&alias, true)?;
    #[cfg(target_os = "linux")]
    FileCompletion::new(
        Arc::new(CompletionMutex::new(CompletionState::<()>::default())),
        Some(Arc::new(OperationFence::default())),
        Some(first.health.clone()),
        Some(first.file.clone()),
        obs::OperationSpan::new(tracing::Span::none()),
    )
    .finish(Err(linux::uncertain_completion(
        io::ErrorKind::TimedOut.into(),
    )));
    #[cfg(not(target_os = "linux"))]
    poison_file_health(&first.file, &first.health);
    drop(first);
    assert!(complete(second.set_len_async(1, None)?).is_err());
    drop(second);
    assert!(
        complete(read_batch_async(
            File::open(&path)?,
            vec![OwnedRead {
                offset: 0,
                length: 1
            }]
        ))
        .is_err()
    );
    let direct = OpenOptions::new().read(true).write(true).open(&alias)?;
    assert!(read_at(&direct, 0, &mut [0]).is_err());
    assert!(write_all_at(&direct, 0, b"x", None).is_err());
    assert!(sync_file(&direct, Durability::Full, None).is_err());
    assert!(sync_data(&direct, Durability::Full).is_err());
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    let independent =
        NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
    complete(independent.set_len_async(1, None)?)?;
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn admitted_job_keeps_alias_registry_alive_after_owner_drop() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("outstanding");
    let alias = temporary.path().join("outstanding-link");
    std::fs::write(&path, [])?;
    let native = bounded_write_file(&path, true)?;
    std::fs::hard_link(&path, &alias)?;
    let (started_tx, started) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let mut pending = native.control_async(
        move |_, _| {
            started_tx.send(()).map_err(io::Error::other)?;
            released.recv().map_err(io::Error::other)?;
            Ok(())
        },
        None,
    )?;
    let waker = completion_waker();
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    started.recv().map_err(io::Error::other)?;
    drop(native);
    let alias = bounded_write_file(&alias, true)?;
    let mut resize = alias.set_len_async(1, None)?;
    assert!(
        Pin::new(&mut resize)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    release.send(()).map_err(io::Error::other)?;
    complete(pending)?;
    complete(resize)?;
    assert_eq!(std::fs::metadata(&path)?.len(), 1);
    Ok(())
}

/// Whether the process registry still holds a row for `file`.
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn registered(file: &File) -> io::Result<bool> {
    let identity = file_identity(file)?;
    let registry = FILE_HEALTH
        .get()
        .ok_or(io::ErrorKind::NotConnected)?
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Ok(registry.find(identity, None)?.is_ok())
}

// The last owner of a healthy file unregisters it, so its health is freed by
// the thread that admitted it. A row left behind would keep the allocation
// until another thread's admission or sweep replaced it, and thread-local
// allocation accounting would see the free land on that thread.
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn last_owner_unregisters_healthy_file_on_its_own_thread() -> io::Result<()> {
    initialize_native_io_services()?;
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    let mut byte = [0; 1];
    read_at(&file, 0, &mut byte)?;
    assert!(!registered(&file)?, "a borrowed admission left its row");
    let native = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    assert!(registered(&file)?);
    // A borrowed admission while an owner is live shares and keeps that owner.
    read_at(&file, 0, &mut byte)?;
    assert!(
        registered(&file)?,
        "a borrowed admission released a live owner"
    );
    drop(native);
    assert!(!registered(&file)?, "the last native owner left its row");
    Ok(())
}

// Queued jobs own the health too. Whichever owner drops last unregisters it:
// here an unpolled read outlives its NativeFile, so the job's owner is last.
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn pending_job_that_outlives_its_file_unregisters_health() -> io::Result<()> {
    initialize_native_io_services()?;
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    let native = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    let read = native.read_batch_async(vec![OwnedRead {
        offset: 0,
        length: 1,
    }]);
    let control = native.control_async(|_, _| Ok(()), None)?;
    drop(native);
    assert!(
        registered(&file)?,
        "a pending job's owner was released early"
    );
    drop(read);
    assert!(
        registered(&file)?,
        "a pending control's owner was released early"
    );
    drop(control);
    assert!(!registered(&file)?, "the last pending job left its row");
    Ok(())
}

// The final two owners dropping at once on different threads: exactly one of
// them takes the owner count to zero and unregisters. Reading the `Arc`'s
// strong count instead let both see the other and both skip cleanup.
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn concurrent_final_owner_drops_unregister_health() -> io::Result<()> {
    initialize_native_io_services()?;
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    let barrier = Arc::new(std::sync::Barrier::new(2));
    for _ in 0..200 {
        let native =
            NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
        let read = native.read_batch_async(vec![OwnedRead {
            offset: 0,
            length: 1,
        }]);
        let other = Arc::clone(&barrier);
        let worker = std::thread::spawn(move || {
            other.wait();
            drop(read);
        });
        barrier.wait();
        drop(native);
        worker
            .join()
            .map_err(|_| io::Error::other("drop thread panicked"))?;
        assert!(!registered(&file)?, "concurrent final owners left the row");
    }
    Ok(())
}

// Negative control: while an owner's release has not run, the check used
// above sees its row. The owner is held, not leaked, so the process-wide
// registry is left clean for later tests once it drops.
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn unreleased_owner_keeps_its_row() -> io::Result<()> {
    initialize_native_io_services()?;
    let temporary = tempfile::NamedTempFile::new()?;
    let file = temporary.reopen()?;
    let native = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    let unreleased = native.health.clone();
    drop(native);
    assert!(registered(&file)?, "an unreleased owner's row was removed");
    drop(unreleased);
    assert!(!registered(&file)?, "the released owner left its row");
    Ok(())
}
