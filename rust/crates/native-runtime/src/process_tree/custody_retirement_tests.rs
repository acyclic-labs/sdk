use super::*;

#[test]
fn controlled_custody_keeps_partial_owners_at_each_admission_boundary() {
    let root = tempfile::tempdir().unwrap();
    let custody = ProcessCustody::open(root.path()).unwrap();
    let mut retained_failures = 0;
    let mut successful = 0;
    for (allowance, failure) in (0..32)
        .map(|n| (n, None))
        .chain((1..=32).map(|n| (u64::MAX, Some(n))))
    {
        let mut command = super::super::tests::command("exit-code", root.path());
        let mut original = crate::tests::ReadBudgetMeter::new(allowance, u64::MAX);
        original.fail_recorded_operation = failure;
        let mut retained = None;
        let result = super::super::ProcessTree::spawn_in_custody_controlled(
            &mut command,
            &custody,
            Some(&original),
            &mut retained,
        );
        successful += usize::from(result.is_ok());
        retained_failures += usize::from(result.is_err() && retained.is_some());
        if let Some(tree) = retained.as_mut() {
            if tree.custody.is_some() {
                assert!(
                    !tree.is_reaped(),
                    "a retained record is still recovery work"
                );
            }
            // A separate, explicitly admitted recovery operation owns cleanup.
            let recovery = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
            tree.terminate(Some(&recovery)).unwrap();
            assert!(tree.is_reaped(), "allowance {allowance}");
        }
        assert_eq!(
            fs::read_dir(root.path()).unwrap().count(),
            0,
            "allowance {allowance}"
        );
    }
    assert!(
        retained_failures > 0,
        "the sweep must cross partial admission"
    );
    assert!(successful > 0, "the sweep must include a complete spawn");
}

#[test]
#[cfg(windows)]
fn failed_record_completion_keeps_the_record_before_job_initialization() {
    let root = tempfile::tempdir().unwrap();
    let custody = ProcessCustody::open(root.path()).unwrap();
    let (record, name) = custody.record().unwrap();
    let mut command = super::super::tests::command("exit-code", root.path());
    let mut retained = None;
    let original = crate::tests::ReadBudgetMeter::new(0, 0);
    let result = super::super::platform::spawn_named(
        &mut command,
        Some(&original),
        &mut retained,
        &name,
        record,
        Err(io::ErrorKind::Interrupted.into()),
        super::super::events::Events::start().unwrap(),
    );
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
    let tree = retained
        .as_mut()
        .expect("physical record has an owned recovery slot");
    assert!(tree.custody.is_some());
    assert!(!tree.is_reaped());
    let recovery = crate::tests::ReadBudgetMeter::new(1, 0);
    tree.terminate(Some(&recovery)).unwrap();
    assert!(tree.is_reaped());
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

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

#[test]
fn failed_retirement_recording_keeps_authority_after_physical_removal() {
    let root = tempfile::tempdir().unwrap();
    let custody = ProcessCustody::open(root.path()).unwrap();
    let mut retained = Some(record(&custody));
    let mut original = crate::tests::ReadBudgetMeter::new(1, 0);
    original.fail_recorded_operation = Some(1);

    assert!(Record::retire(&mut retained, Some(&original)).is_err());
    assert!(
        retained.is_some(),
        "physical success alone cannot settle custody"
    );
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);

    // The retained authority also handles an already absent record. This is
    // a new admitted recovery operation, not a repeat on the failed ledger.
    let recovery = crate::tests::ReadBudgetMeter::new(1, 0);
    Record::retire(&mut retained, Some(&recovery)).unwrap();
    assert!(retained.is_none());
    assert_eq!(
        recovery
            .used_operations
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );
}
