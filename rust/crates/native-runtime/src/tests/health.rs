use super::*;

#[cfg(windows)]
#[test]
fn healthy_registry_does_not_retain_exclusive_handle() -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("exclusive-native-file");
    let file = OpenOptions::new().create_new(true).read(true).write(true).share_mode(0).open(&path)?;
    let native = NativeFile::from_file(file, None).map_err(|(error, _)| error)?;
    drop(native);
    File::open(&path)?;
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn independently_opened_aliases_share_fencing_and_uncertainty() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("identity");
    let alias = temporary.path().join("hard-link");
    let first = NativeFile::from_file(OpenOptions::new().create_new(true).read(true).write(true).open(&path)?, None)
        .map_err(|(error, _)| error)?;
    std::fs::hard_link(&path, &alias)?;
    let second = NativeFile::from_file(OpenOptions::new().read(true).write(true).open(&alias)?, None)
        .map_err(|(error, _)| error)?;
    poison_file_health(&first.file, &first.health);
    drop(first);
    assert!(complete(second.set_len_async(1, None)?).is_err(), "an alias bypassed an uncertain completion");
    drop(second);
    assert!(complete(read_batch_async(
        OpenOptions::new().read(true).open(&path)?, vec![OwnedRead { offset: 0, length: 1 }],
    )).is_err(), "the free read API bypassed the file fence");
    let direct_alias = OpenOptions::new().read(true).write(true).open(&alias)?;
    let mut byte = [0];
    assert!(read_at(&direct_alias, 0, &mut byte).is_err());
    assert!(write_all_at(&direct_alias, 0, b"x", None).is_err());
    assert!(sync_file(&direct_alias, Durability::Full, None).is_err());
    assert!(sync_data(&direct_alias, Durability::Full).is_err());
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    let independent = NativeFile::from_file(
        OpenOptions::new().create_new(true).read(true).write(true).open(temporary.path().join("clean"))?, None,
    ).map_err(|(error, _)| error)?;
    complete(independent.set_len_async(1, None)?)?;
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn admitted_job_keeps_alias_registry_alive_after_owner_drop() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("outstanding");
    let alias_path = temporary.path().join("outstanding-link");
    let native = NativeFile::from_file(OpenOptions::new().create_new(true).read(true).write(true).open(&path)?, None)
        .map_err(|(error, _)| error)?;
    std::fs::hard_link(&path, &alias_path)?;
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel::<()>(1);
    let mut pending = native.control_async(move |_, _| {
        started_sender.send(()).map_err(io::Error::other)?;
        release_receiver.recv().map_err(io::Error::other)?;
        Ok(())
    }, None)?;
    let waker = completion_waker();
    assert!(matches!(Pin::new(&mut pending).poll(&mut Context::from_waker(&waker)), Poll::Pending));
    started_receiver.recv_timeout(Duration::from_secs(5)).map_err(io::Error::other)?;
    drop(native);
    let alias = NativeFile::from_file(OpenOptions::new().read(true).write(true).open(&alias_path)?, None)
        .map_err(|(error, _)| error)?;
    let mut resize = alias.set_len_async(1, None)?;
    assert!(matches!(Pin::new(&mut resize).poll(&mut Context::from_waker(&waker)), Poll::Pending));
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    release_sender.send(()).map_err(io::Error::other)?;
    complete(pending)?;
    complete(resize)?;
    assert_eq!(std::fs::metadata(&path)?.len(), 1);
    Ok(())
}
