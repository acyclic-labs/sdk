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
        Arc::new(Mutex::new(CompletionState::<()>::default())),
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
    started
        .recv_timeout(Duration::from_secs(5))
        .map_err(io::Error::other)?;
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
