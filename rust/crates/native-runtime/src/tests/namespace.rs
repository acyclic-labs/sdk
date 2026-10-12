use super::*;

#[test]
fn a_service_that_exits_without_signalling_closes_its_readiness_channel() -> io::Result<()> {
    // This test binary rejects the service arguments and exits, writing
    // only to its discarded standard error.
    let readiness = spawn_service_process(&std::env::current_exe()?)?;
    assert!(!readiness.wait()?);
    Ok(())
}

#[test]
fn a_process_not_started_as_a_signalled_service_has_no_readiness_channel() -> io::Result<()> {
    assert!(take_service_ready_signal()?.is_none());
    Ok(())
}

#[test]
fn exclusive_lock_contention_is_not_a_generic_io_failure() {
    assert!(is_exclusive_lock_contention(&io::Error::from(
        io::ErrorKind::WouldBlock
    )));
    assert!(!is_exclusive_lock_contention(&io::Error::from(
        io::ErrorKind::PermissionDenied
    )));
    assert!(!is_exclusive_lock_contention(&io::Error::from(
        io::ErrorKind::Unsupported
    )));
    #[cfg(windows)]
    assert!(is_exclusive_lock_contention(&io::Error::from_raw_os_error(
        33
    )));
}

#[test]
fn durable_rename_moves_and_replaces_entries() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source");
    let destination = directory.path().join("destination");
    std::fs::write(&source, b"first")?;
    durable_rename(&source, &destination, RenameMode::NoReplace, None)?;
    assert!(!source.exists());
    assert_eq!(std::fs::read(&destination)?, b"first");

    std::fs::write(&source, b"second")?;
    durable_rename(&source, &destination, RenameMode::Replace, None)?;
    assert!(!source.exists());
    assert_eq!(std::fs::read(destination)?, b"second");
    Ok(())
}

#[cfg(unix)]
#[test]
fn durable_rename_accepts_relative_sibling_paths() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let original = std::env::current_dir()?;
    std::env::set_current_dir(temporary.path())?;
    let result = (|| {
        std::fs::write("from", b"relative")?;
        durable_rename(Path::new("from"), Path::new("to"), RenameMode::NoReplace, None)?;
        assert_eq!(std::fs::read("to")?, b"relative");
        Ok(())
    })();
    std::env::set_current_dir(original)?;
    result
}

#[test]
fn no_replace_preserves_both_entries() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source");
    let destination = directory.path().join("destination");
    std::fs::write(&source, b"source")?;
    std::fs::write(&destination, b"destination")?;
    let Err(error) = durable_rename(&source, &destination, RenameMode::NoReplace, None) else {
        return Err(io::Error::other(
            "existing destination accepted no-replace rename",
        ));
    };
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(publish_new(&source, &destination, None).unwrap_err().kind(), io::ErrorKind::AlreadyExists);
    let mut limits = ReadBudgetMeter::new(0, 0);
    limits.memory_limit = 0;
    let observer = Arc::new(limits);
    let meter: Arc<dyn NativeReadMeter> = observer.clone();
    assert_eq!(publish_new(&source, &destination, Some(&meter)).unwrap_err().kind(), io::ErrorKind::Other);
    assert_eq!(durable_rename(
        &source, &destination, RenameMode::Replace, Some(&meter),
    ).unwrap_err().kind(), io::ErrorKind::Other);
    assert_eq!(observer.memory.load(Ordering::Acquire), 0);
    assert_eq!(std::fs::read(source)?, b"source");
    assert_eq!(std::fs::read(destination)?, b"destination");
    Ok(())
}
