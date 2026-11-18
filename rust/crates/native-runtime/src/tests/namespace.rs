use super::*;

#[cfg(windows)]
mod windows;

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
    assert_eq!(
        publish_new(&source, &destination, None).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    let mut limits = ReadBudgetMeter::new(0, 0);
    limits.memory_limit = 0;
    let observer = Arc::new(limits);
    let meter: Arc<dyn NativeReadMeter> = observer.clone();
    assert_eq!(
        publish_new(&source, &destination, Some(&meter))
            .unwrap_err()
            .kind(),
        io::ErrorKind::Other
    );
    assert_eq!(
        durable_rename(&source, &destination, RenameMode::Replace, Some(&meter),)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Other
    );
    assert_eq!(observer.memory.load(Ordering::Acquire), 0);
    #[cfg(windows)]
    // Both normalized paths, the rename packet, and the source open precede
    // the actual rename admission. A refused prefix must not report that write
    // or its durability attempt, invoke the custody callback, or retain credit.
    for (allocation_limit, operation_limit, present) in (0..=4)
        .map(|limit| (limit, u64::MAX, true))
        .chain((0..=2).map(|limit| (u64::MAX, limit, true)))
        .chain([(u64::MAX, u64::MAX, false)])
    {
        let mut limits = ReadBudgetMeter::new(operation_limit, 0);
        limits.allocation_limit = allocation_limit;
        let observer = Arc::new(limits);
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let missing = directory.path().join("missing");
        let renamed = std::cell::Cell::new(false);
        let error = crate::namespace::durable_rename_observed(
            if present { &source } else { &missing },
            &destination,
            RenameMode::Replace,
            Some(&meter),
            || renamed.set(true),
        )
        .unwrap_err();
        assert_eq!(
            error.kind(),
            if present {
                io::ErrorKind::Other
            } else {
                io::ErrorKind::NotFound
            },
        );
        assert_eq!(
            observer.used_operations.load(Ordering::Acquire),
            u64::from(!present || (operation_limit > 0 && operation_limit < 3))
        );
        assert_eq!(observer.memory.load(Ordering::Acquire), 0);
        assert!(!renamed.get());
        assert_eq!(std::fs::read(&source)?, b"source");
        assert_eq!(std::fs::read(&destination)?, b"destination");
    }
    assert_eq!(std::fs::read(source)?, b"source");
    assert_eq!(std::fs::read(destination)?, b"destination");
    Ok(())
}
