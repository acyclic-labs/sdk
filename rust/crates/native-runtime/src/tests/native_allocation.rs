use super::*;

#[repr(align(64))]
struct Aligned {
    _byte: u8,
}

fn actual_arc<T>(value: T) -> io::Result<()> {
    let mut allocation = None;
    let created = acyclic_test_support::measure(|| {
        allocation = Some(std::hint::black_box(Arc::new(value)));
    });
    assert_eq!(created.count_current, 1);
    assert_eq!(
        usize::try_from(created.bytes_current).map_err(|_| io::ErrorKind::InvalidData)?,
        arc_allocation_bytes::<T>(),
        "shared Arc footprint omitted counters or alignment"
    );
    let released = acyclic_test_support::measure(|| drop(allocation));
    assert_eq!(released.count_current, -1);
    assert_eq!(released.bytes_current, -created.bytes_current);
    Ok(())
}

#[test]
fn shared_arc_footprint_matches_the_allocator_for_zero_small_and_aligned_values() -> io::Result<()>
{
    actual_arc(())?;
    actual_arc(7_u8)?;
    actual_arc([7_u64; 5])?;
    actual_arc(Aligned { _byte: 7 })
}

#[test]
fn ordinary_completion_funds_the_complete_fence_allocation() -> io::Result<()> {
    let file = NativeFile::from_file(tempfile::tempfile()?, None).map_err(|(error, _)| error)?;
    let observed = Arc::new(ReadBudgetMeter::new(1, 0));
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let mut task = file.control_async(|_, _| Ok(()), Some(meter))?;
    let baseline = observed.memory.load(Ordering::Acquire);
    let mut result = Ok(());
    let created = acyclic_test_support::measure(|| {
        result = crate::completion::reserve_if_needed(
            &mut task.predecessor,
            &mut task.completion,
            &mut task.pending,
        );
    });
    result?;
    assert_eq!(created.count_current, 1);
    assert_eq!(
        observed.memory.load(Ordering::Acquire) - baseline,
        u64::try_from(created.bytes_current).map_err(|_| io::ErrorKind::InvalidData)?,
        "ordinary completion omitted fence Arc counters"
    );
    drop(task);
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn blocking_wait_funds_the_complete_notifier_allocation() -> io::Result<()> {
    let observed = Arc::new(ReadBudgetMeter::new(0, 0));
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let mut wake = None;
    let created = acyclic_test_support::measure(|| {
        wake = Some(crate::blocking::allocate_blocking_wait_wake(&meter));
    });
    let wake = wake.ok_or(io::ErrorKind::Other)??;
    assert_eq!(created.count_current, 1);
    assert_eq!(
        observed.memory.load(Ordering::Acquire),
        u64::try_from(created.bytes_current).map_err(|_| io::ErrorKind::InvalidData)?,
        "blocking wait omitted notifier Arc counters"
    );
    drop(wake);
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn retained_waker_keeps_original_notifier_funding_after_wait_returns() -> io::Result<()> {
    let observed = Arc::new(ReadBudgetMeter::new(0, 0));
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let prepared = crate::blocking::allocate_blocking_wait_wake(&meter)?;
    let funded = observed.memory.load(Ordering::Acquire);
    assert!(funded > 0);
    let mut retained = None;
    let task = std::future::poll_fn(|context| {
        retained = Some(context.waker().clone());
        Poll::Ready(Ok(()))
    });
    crate::blocking::wait_measured_completion(
        task,
        &meter,
        Some(prepared),
        None,
        std::future::pending(),
        || {},
    )?;
    assert_eq!(
        observed.memory.load(Ordering::Acquire),
        funded,
        "retained waker lost the notifier's original funding"
    );
    let retained = retained.ok_or(io::ErrorKind::Other)?;
    retained.wake_by_ref();
    drop(retained);
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn task_completion_funds_the_complete_state_allocation() -> io::Result<()> {
    let observed = Arc::new(ReadBudgetMeter::new(0, 0));
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let task =
        crate::blocking::task_completion(|| Ok(()), Some(meter), None, None, &BLOCKING_WORKERS)?;
    let mut control = None;
    let created = acyclic_test_support::measure(|| {
        control = Some(Arc::new(CompletionMutex::new(
            CompletionState::<()>::default(),
        )));
    });
    assert_eq!(created.count_current, 1);
    let state = task.state.as_ref().ok_or(io::ErrorKind::Other)?;
    let funded = state
        .lock()
        .map_err(|_| io::ErrorKind::Other)?
        ._allocation
        .as_ref()
        .ok_or(io::ErrorKind::Other)?
        .bytes();
    assert_eq!(
        funded,
        u64::try_from(created.bytes_current).map_err(|_| io::ErrorKind::InvalidData)?,
        "task completion omitted state Arc counters"
    );
    drop(control);
    drop(task);
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn retained_completion_state_keeps_funding_after_result_or_observer_drop() -> io::Result<()> {
    for abandon in [false, true] {
        let observed = Arc::new(ReadBudgetMeter::new(0, 0));
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let mut task = crate::blocking::task_completion(
            || Ok(17),
            Some(meter),
            None,
            None,
            &BLOCKING_WORKERS,
        )?;
        let state = task.state.clone().ok_or(io::ErrorKind::Other)?;
        let funded = state
            .lock()
            .map_err(|_| io::ErrorKind::Other)?
            ._allocation
            .as_ref()
            .ok_or(io::ErrorKind::Other)?
            .bytes();
        let job = task.pending.take().ok_or(io::ErrorKind::Other)?;
        let mut observer = Some(task);
        if abandon {
            drop(observer.take());
        }
        wake_capacity_waiter(job.run());
        if let Some(task) = observer.as_mut() {
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(
                Pin::new(task).poll(&mut context),
                Poll::Ready(Ok(17))
            ));
        }
        drop(observer);
        assert_eq!(
            observed.memory.load(Ordering::Acquire),
            funded,
            "retained completion state lost its original funding"
        );
        drop(state);
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn retired_waiter_keeps_full_funding_until_the_last_notifier_drops() -> io::Result<()> {
    for queued in [false, true] {
        let observed = Arc::new(ReadBudgetMeter::new(0, 0));
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let allocation = prepay_capacity_waiter(&meter)?;
        let funded = allocation.bytes();
        let mut node = None;
        let actual = acyclic_test_support::measure(|| {
            node = Some(capacity_waiter(Some(allocation)));
        });
        let node = node.ok_or(io::ErrorKind::Other)?;
        assert_eq!(actual.count_current, 1);
        assert_eq!(
            funded,
            u64::try_from(actual.bytes_current).map_err(|_| io::ErrorKind::InvalidData)?,
            "capacity waiter omitted its Arc counters"
        );
        let mut admission = AdmissionState::new();
        if queued {
            admission.register_existing(&node, Waker::noop());
        }
        let retained = Arc::clone(&node);
        wake_capacity_waiter(admission.retire_owner(node));
        assert_eq!(admission.waiter_count, 0);
        assert_eq!(
            observed.memory.load(Ordering::Acquire),
            funded,
            "retired shared waiter lost its original funding"
        );
        drop(retained);
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn retired_waiter_does_not_retain_neighbor_allocations() -> io::Result<()> {
    let observed = Arc::new(ReadBudgetMeter::new(0, 0));
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let mut admission = AdmissionState::new();
    let first = capacity_waiter(Some(prepay_capacity_waiter(&meter)?));
    let retained = capacity_waiter(Some(prepay_capacity_waiter(&meter)?));
    let last = capacity_waiter(Some(prepay_capacity_waiter(&meter)?));
    let funded = observed.memory.load(Ordering::Acquire) / 3;
    for node in [&first, &retained, &last] {
        admission.register_existing(node, Waker::noop());
    }
    let retired = acyclic_test_support::measure(|| {
        wake_capacity_waiter(admission.retire_owner(Arc::clone(&retained)));
        wake_capacity_waiter(admission.retire_owner(first));
        wake_capacity_waiter(admission.retire_owner(last));
    });
    assert_eq!(admission.waiter_count, 0);
    let state = retained.state.lock().map_err(|_| io::ErrorKind::Other)?;
    assert!(
        state.previous.upgrade().is_none() && state.next.is_none(),
        "retired waiter retained its former neighbors"
    );
    drop(state);
    assert_eq!(
        observed.memory.load(Ordering::Acquire),
        funded,
        "retired waiter retained another node's allocation"
    );
    assert_eq!(
        retired.count_current, -2,
        "retired waiter kept a neighbor's physical Arc allocation"
    );
    assert_eq!(
        retired.bytes_current,
        -i64::try_from(funded.checked_mul(2).ok_or(io::ErrorKind::InvalidData)?)
            .map_err(|_| io::ErrorKind::InvalidData)?,
        "retired waiter kept a neighbor's physical Arc allocation"
    );
    drop(retained);
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn both_file_import_forms_fund_the_complete_payload_allocation() -> io::Result<()> {
    initialize_native_io_services()?;
    let temporary = tempfile::NamedTempFile::new()?;
    // Keep the shared identity row live: each following import creates only
    // its own payload Arc and borrows the existing health owner.
    let previous = NativeFile::from_file(temporary.reopen()?, None).map_err(|(error, _)| error)?;
    for owned in [false, true] {
        let observed = Arc::new(ReadBudgetMeter::new(1, 0));
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let file = temporary.reopen()?;
        let accounting = if owned {
            NativeFileMeter::Owned(meter)
        } else {
            NativeFileMeter::Borrowed(observed.as_ref())
        };
        let mut imported = None;
        let actual = acyclic_test_support::measure(|| {
            imported = Some(NativeFile::from_file(file, Some(accounting)));
        });
        let imported = imported
            .ok_or(io::ErrorKind::Other)?
            .map_err(|(error, _)| error)?;
        assert!(Arc::ptr_eq(&previous.health, &imported.health));
        assert_eq!(actual.count_current, 1);
        let physical =
            u64::try_from(actual.bytes_current).map_err(|_| io::ErrorKind::InvalidData)?;
        assert_eq!(
            observed.memory.load(Ordering::Acquire),
            physical,
            "native file import omitted its Arc allocation counters"
        );
        assert_eq!(imported.logical_owned_bytes(), physical);
        drop(imported);
        if !owned {
            observed.release(physical);
        }
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[test]
fn file_import_cannot_spend_payload_only_credit_before_querying_the_file() -> io::Result<()> {
    initialize_native_io_services()?;
    for owned in [false, true] {
        let mut policy = ReadBudgetMeter::new(1, 0);
        policy.memory_limit = std::mem::size_of::<NativeFilePayload>() as u64;
        let observed = Arc::new(policy);
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let accounting = if owned {
            NativeFileMeter::Owned(meter)
        } else {
            NativeFileMeter::Borrowed(observed.as_ref())
        };
        let result = NativeFile::from_file(tempfile::tempfile()?, Some(accounting));
        assert!(result.is_err(), "native import spent unfunded Arc counters");
        assert_eq!(
            observed.used_operations.load(Ordering::Acquire),
            0,
            "native import spent unfunded Arc counters before querying the file"
        );
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}
