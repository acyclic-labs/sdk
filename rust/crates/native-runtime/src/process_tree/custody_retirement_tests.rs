use super::*;

fn record(custody: &ProcessCustody) -> Record {
    #[cfg(unix)]
    {
        let pid = libc::pid_t::MAX;
        fs::write(custody.path.join(pid.to_string()), []).unwrap();
        Record::new(File::open(&custody.path).unwrap(), pid)
    }
    #[cfg(windows)]
    {
        custody.record().unwrap().0
    }
}

#[test]
fn denied_record_retirement_keeps_recovery_authority_on_the_original_ledger() {
    let root = tempfile::tempdir().unwrap();
    let custody = ProcessCustody::open(root.path()).unwrap();
    let mut retained = Some(record(&custody));
    let denied = crate::tests::ReadBudgetMeter::new(0, 0);
    assert!(Record::retire(&mut retained, Some(&denied)).is_err());
    assert!(retained.is_some());
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);

    // Recovery still owns the exact record and retires it on its own original
    // admitted operation, without substituting an unmetered cleanup path.
    let recovery = crate::tests::ReadBudgetMeter::new(1, 0);
    Record::retire(&mut retained, Some(&recovery)).unwrap();
    assert!(retained.is_none());
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    assert_eq!(
        recovery
            .used_operations
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );

    // Restore the former take-before-remove shape under the same denial. The
    // durable record survives, but its owned recovery authority is lost.
    let mut old = Some(record(&custody));
    let removed = old.take().unwrap();
    assert!(
        super::super::process_control_attempt(
            Some(&denied),
            crate::NativeOperationKind::Write,
            || removed.remove()
        )
        .is_err()
    );
    assert!(old.is_none());
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    removed.remove().unwrap();
}

#[test]
fn physical_record_removal_failure_keeps_the_same_owned_recovery_record() {
    let root = tempfile::tempdir().unwrap();
    let custody = ProcessCustody::open(root.path()).unwrap();
    let mut retained = Some(record(&custody));
    let path = fs::read_dir(root.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    let original = crate::tests::ReadBudgetMeter::new(1, 0);
    assert!(Record::retire(&mut retained, Some(&original)).is_err());
    assert!(retained.is_some());
    assert!(path.is_dir());
    assert_eq!(
        original
            .used_operations
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );
    fs::remove_dir(path).unwrap();
    let recovery = crate::tests::ReadBudgetMeter::new(1, 0);
    Record::retire(&mut retained, Some(&recovery)).unwrap();
    assert!(retained.is_none());
}
