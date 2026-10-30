use super::*;
use std::sync::atomic::Ordering;

enum HostState<T> {
    Idle,
    Running(T),
}

#[test]
fn bounded_poll_scopes_preserve_original_storage_and_retirement_credit() {
    let root = tempfile::tempdir().unwrap();
    let custody = ProcessCustody::open(root.path()).unwrap();
    let original = Arc::new(crate::tests::ReadBudgetMeter::new(256, 65536));
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let mut slot = ProcessTreeStorage::new(&observer).unwrap();
    slot.spawn(
        &mut super::super::tests::command("partial", root.path()),
        Some(&custody),
    )
    .unwrap();
    super::super::tests::ready(root.path(), "partial-ready", || {
        slot.try_wait().unwrap().is_some()
    });
    let before = original.used_operations.load(Ordering::Acquire);
    let memory = original.memory.load(Ordering::Acquire);
    for _ in 0..1024 {
        let scope = Arc::new(crate::tests::ReadBudgetMeter::new(16, 65536));
        let meter: Arc<dyn crate::NativeReadMeter> = scope.clone();
        let mut io = slot.io(&meter).unwrap();
        assert!(io.try_wait().unwrap().is_none());
        io.read_stdout(&mut [0; 1024]).unwrap();
        assert!(!io.drain_stderr().unwrap().closed);
        assert_eq!(
            original.used_operations.load(Ordering::Acquire),
            before,
            "polling consumed the lifetime allowance"
        );
        assert!(scope.used_operations.load(Ordering::Acquire) > 0);
        assert_eq!(original.memory.load(Ordering::Acquire), memory);
    }
    assert_eq!(slot.stderr_tail(), b"stderr-before-stop");
    original.cancelled.store(true, Ordering::Release);
    drop(slot);
    custody.fence_stale().unwrap();
    assert_eq!(original.memory.load(Ordering::Acquire), 0);
}

#[test]
fn stderr_tail_is_bounded_and_drained_after_stdout_closes() {
    let root = tempfile::tempdir().unwrap();
    let original = Arc::new(crate::tests::ReadBudgetMeter::new(256, 65536));
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let mut slot = ProcessTreeStorage::new(&observer).unwrap();
    let mut command = super::super::tests::command("stderr-bulk", root.path());
    command.stdout(std::process::Stdio::null());
    slot.spawn(&mut command, None).unwrap();
    let mut read_bytes = 0;
    loop {
        let meter: Arc<dyn crate::NativeReadMeter> =
            Arc::new(crate::tests::ReadBudgetMeter::new(32, 65536));
        let mut io = slot.io(&meter).unwrap();
        assert_eq!(io.read_stdout(&mut [0; 1024]).unwrap(), Some(0));
        let drain = io.drain_stderr().unwrap();
        read_bytes += drain.read_bytes;
        if drain.closed {
            break;
        }
        if drain.read_bytes == 0 {
            io.readiness().unwrap().0.block();
        }
    }
    let status = slot
        .slot_mut()
        .unwrap()
        .as_mut()
        .unwrap()
        .wait_exit(Some(original.as_ref()))
        .unwrap();
    assert_eq!(status.code(), Some(7));
    assert!(
        read_bytes >= 262_144,
        "stderr progress was hidden from the scheduler"
    );
    assert_eq!(slot.stderr_tail().len(), 4096);
    assert!(slot.stderr_tail().ends_with(b"stderr-tail-marker"));
    slot.terminate().unwrap();
}

#[test]
fn process_storage_is_small_and_matches_the_original_heap_credit() {
    const CEILING: usize = 64;
    let before = std::mem::size_of::<HostState<ProcessTree>>();
    let after = std::mem::size_of::<HostState<ProcessTreeStorage>>();
    println!("process host state: {before} -> {after} bytes");
    assert!(after <= CEILING);
    assert!(
        before > CEILING,
        "the actual inline owner is the negative control"
    );

    let original = Arc::new(crate::tests::ReadBudgetMeter::new(0, 0));
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let mut state = HostState::Idle;
    let physical = acyclic_test_support::measure(|| {
        state = HostState::Running(ProcessTreeStorage::new(&observer).unwrap());
    });
    assert_eq!(
        u64::try_from(physical.bytes_current).unwrap(),
        original.memory.load(Ordering::Acquire)
    );
    assert_eq!(
        usize::try_from(physical.bytes_current).unwrap(),
        std::mem::size_of::<ProcessSlot>() + 4096
    );
    let HostState::Running(slot) = &state else {
        panic!("missing funded slot")
    };
    assert!(slot.is_reaped());
    drop(state);
    assert_eq!(original.memory.load(Ordering::Acquire), 0);
}

#[test]
fn denied_process_storage_never_reaches_an_allocator_or_spawn() {
    let original = Arc::new(crate::tests::ReadBudgetMeter::new(0, 0));
    original.deny_allocations.store(true, Ordering::Release);
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let physical = acyclic_test_support::measure(|| {
        assert!(ProcessTreeStorage::new(&observer).is_err());
    });
    assert_eq!(physical.bytes_total, 0);
    assert_eq!(original.memory.load(Ordering::Acquire), 0);
    assert_eq!(original.used_operations.load(Ordering::Acquire), 0);
}

#[test]
fn malformed_private_storage_cannot_acknowledge_quiescence_or_spawn() {
    let original = Arc::new(crate::tests::ReadBudgetMeter::new(0, 0));
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let mut storage = ProcessTreeStorage::new(&observer).unwrap();
    // Public operations cannot resize the slot. Exercise the guard against a
    // future internal change, rather than allowing an invalid owner to appear idle.
    storage.slot.data.clear();
    assert!(!storage.is_reaped());
    assert_eq!(
        storage.terminate().unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    let mut command = Command::new("must-not-start");
    assert_eq!(
        storage.spawn(&mut command, None).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(original.used_operations.load(Ordering::Acquire), 0);
}

#[test]
fn native_session_events_retain_the_original_meter_and_process_owner() {
    let root = tempfile::tempdir().unwrap();
    let custody = ProcessCustody::open(root.path()).unwrap();
    let original = Arc::new(crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX));
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let mut slot = ProcessTreeStorage::new(&observer).unwrap();
    let mut command = super::super::tests::command("stdin-save", root.path());
    command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    slot.spawn(&mut command, Some(&custody)).unwrap();
    let before = original.used_operations.load(Ordering::Acquire);
    slot.queue_stdin(b"one retained native prompt".to_vec())
        .unwrap();
    original.cancelled.store(true, Ordering::Release);
    assert_eq!(
        slot.pump_stdin().unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    assert!(
        !slot.is_reaped(),
        "refused input cannot drop process custody"
    );
    original.cancelled.store(false, Ordering::Release);
    while !slot.pump_stdin().unwrap() {
        slot.io(&observer).unwrap().readiness().unwrap().0.block();
    }
    slot.require_stdin_complete().unwrap();
    assert!(
        slot.slot_mut()
            .unwrap()
            .as_mut()
            .unwrap()
            .wait_exit(Some(original.as_ref()))
            .unwrap()
            .success()
    );
    assert_eq!(
        std::fs::read(root.path().join("stdin-captured")).unwrap(),
        b"one retained native prompt"
    );
    slot.terminate_descendants().unwrap();
    slot.terminate().unwrap();
    assert!(slot.is_reaped());
    assert!(original.used_operations.load(Ordering::Acquire) > before);
}

#[test]
fn failed_spawn_keeps_the_partial_tree_and_its_storage_on_the_original_owner() {
    let root = tempfile::tempdir().unwrap();
    let custody = ProcessCustody::open(root.path()).unwrap();
    let mut meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    // On Windows this fails the physical record's recording; on Unix it
    // fails the armed spawn's recording. Both return an owned partial slot.
    meter.fail_recorded_operation =
        Some(platform::RETIREMENT_WRITES + 1 + if cfg!(windows) { 1 } else { 2 });
    let original = Arc::new(meter);
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let mut slot = ProcessTreeStorage::new(&observer).unwrap();
    let retained = original.memory.load(Ordering::Acquire);
    let mut command = super::super::tests::command("exit-code", root.path());
    assert!(slot.spawn(&mut command, Some(&custody)).is_err());
    assert!(!slot.is_reaped());
    assert_eq!(original.memory.load(Ordering::Acquire), retained);
    let before_cleanup = original.used_operations.load(Ordering::Acquire);
    slot.terminate().unwrap();
    assert!(slot.is_reaped());
    assert!(original.used_operations.load(Ordering::Acquire) > before_cleanup);
    assert_eq!(
        slot.spawn(&mut command, Some(&custody)).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists,
        "retired storage cannot silently become another execution"
    );
    assert_eq!(original.memory.load(Ordering::Acquire), retained);
    drop(slot);
    assert_eq!(original.memory.load(Ordering::Acquire), 0);
}

#[test]
#[cfg(any(target_os = "linux", windows))]
fn dropping_cancelled_original_owner_stops_and_reaps_before_releasing_credit() {
    let root = tempfile::tempdir().unwrap();
    let custody = ProcessCustody::open(&root.path().join("custody")).unwrap();
    let original = Arc::new(crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX));
    let observer: Arc<dyn crate::NativeReadMeter> = original.clone();
    let mut slot = ProcessTreeStorage::new(&observer).unwrap();
    slot.spawn(
        &mut super::super::tests::command("child", root.path()),
        Some(&custody),
    )
    .unwrap();
    super::super::tests::ready(root.path(), "tree-ready", || {
        slot.try_wait().unwrap().is_some()
    });
    let pid = slot
        .slot_mut()
        .unwrap()
        .as_ref()
        .unwrap()
        .child
        .as_ref()
        .unwrap()
        .id();
    let watch = super::super::custody_tests::Watch::new(pid).unwrap();
    assert!(!watch.exited());
    original.cancelled.store(true, Ordering::Release);
    assert_eq!(
        slot.terminate().unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    assert!(original.memory.load(Ordering::Acquire) > 0);
    drop(slot);
    assert!(
        watch.exited(),
        "the old owner drops its credit with the child still running"
    );
    custody.fence_stale().unwrap();
    assert_eq!(
        std::fs::read_dir(root.path().join("custody"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(original.memory.load(Ordering::Acquire), 0);
}
