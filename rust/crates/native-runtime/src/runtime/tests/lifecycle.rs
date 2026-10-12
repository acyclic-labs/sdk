use super::*;

#[test]
fn a_service_that_exits_without_signalling_closes_its_readiness_channel() -> io::Result<()> {
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
fn ownership_release_barrier_waits_for_the_final_clone() {
    let anchor = OwnershipAnchor::new(());
    let clone = anchor.clone();
    let barrier = anchor.release_barrier();
    drop(anchor);
    assert!(!barrier.is_released());
    drop(clone);
    barrier.wait();
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
        durable_rename(
            Path::new("from"),
            Path::new("to"),
            RenameMode::NoReplace,
            None,
        )?;
        assert_eq!(std::fs::read("to")?, b"relative");
        Ok(())
    })();
    std::env::set_current_dir(original)?;
    result
}

#[cfg(not(target_vendor = "apple"))]
#[test]
fn barrier_durability_rejects_targets_without_an_exact_primitive() -> io::Result<()> {
    let temporary = tempfile::tempdir()?;
    let file = File::create(temporary.path().join("barrier"))?;
    assert!(matches!(
        sync_file(&file, Durability::Barrier, None),
        Err(error) if error.kind() == io::ErrorKind::Unsupported
    ));
    assert!(matches!(
        sync_data(&file, Durability::Barrier),
        Err(error) if error.kind() == io::ErrorKind::Unsupported
    ));
    Ok(())
}
