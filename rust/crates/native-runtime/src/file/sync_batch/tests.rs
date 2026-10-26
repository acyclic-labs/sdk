use super::*;
use crate::tests::{ReadBudgetMeter, complete};
use std::io::Write as _;
use std::sync::atomic::AtomicUsize;

type TestState = Arc<CompletionMutex<CompletionState<()>>>;

fn require<T>(value: Option<T>, message: &'static str) -> io::Result<T> {
    value.ok_or_else(|| io::Error::other(message))
}

fn meter() -> Arc<ReadBudgetMeter> {
    Arc::new(ReadBudgetMeter::new(1000, 1024 * 1024))
}

fn files(count: usize, meter: &Arc<dyn NativeReadMeter>) -> io::Result<Vec<NativeFile>> {
    (0..count)
        .map(|_| {
            let mut file = tempfile::tempfile()?;
            file.write_all(b"durable contents")?;
            NativeFile::from_file(file, Some(NativeFileMeter::Owned(Arc::clone(meter))))
                .map_err(|(error, _)| error)
        })
        .collect()
}

fn finish(states: &[TestState], index: usize, failures: usize) -> io::Result<()> {
    let result = if failures & (1 << index) == 0 {
        Ok(())
    } else {
        Err(io::Error::other(index.to_string()))
    };
    require(states.get(index), "missing member state")?
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .result = Some(result);
    Ok(())
}

fn terminal_order(reverse: bool, failures: usize, initial: usize) -> io::Result<()> {
    let concrete = meter();
    let meter: Arc<dyn NativeReadMeter> = concrete.clone();
    let states: Vec<_> = (0..2)
        .map(|_| Arc::new(CompletionMutex::new(CompletionState::default())))
        .collect();
    let mut batch = NativeSyncBatch::prepare(2, &meter, |index| {
        let mut member = NativeCompletion::ready(Ok(()));
        member.ready = None;
        member.state = Some(require(states.get(index), "missing prepared state")?.clone());
        Ok(member)
    })?;
    let mut done = initial;
    for index in 0..2 {
        if done & (1 << index) != 0 {
            finish(&states, index, failures)?;
        }
    }
    let mut context = Context::from_waker(Waker::noop());
    let result = loop {
        let result = Pin::new(&mut batch).poll(&mut context);
        if done == 3 {
            break result;
        }
        assert!(result.is_pending(), "acknowledged an unfinished member");
        for (index, state) in states.iter().enumerate() {
            if done & (1 << index) == 0 {
                assert!(
                    state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .waker
                        .is_some(),
                    "serialized member {index}"
                );
            }
        }
        let index = if reverse {
            (0..2).rev().find(|index| done & (1 << index) == 0)
        } else {
            (0..2).find(|index| done & (1 << index) == 0)
        };
        let index = require(index, "unfinished member was lost")?;
        finish(&states, index, failures)?;
        done |= 1 << index;
    };
    let Poll::Ready(result) = result else {
        return Err(io::Error::other("lost physical terminals"));
    };
    if failures == 0 {
        result?;
    } else {
        assert_eq!(
            require(result.err(), "lost member failure")?.to_string(),
            if failures & 1 != 0 { "0" } else { "1" }
        );
    }
    drop(batch);
    assert_eq!(concrete.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn batch_polls_every_member_and_drains_every_terminal_order() -> io::Result<()> {
    initialize_native_io_services()?;
    // Both orders, all failure subsets and all initially visible terminals.
    for reverse in [false, true] {
        for failures in 0..4 {
            for initial in 0..4 {
                terminal_order(reverse, failures, initial)?;
            }
        }
    }
    Ok(())
}

#[test]
fn batch_denied_preparation_starts_nothing_and_retires_backing() -> io::Result<()> {
    initialize_native_io_services()?;
    let concrete = meter();
    let meter: Arc<dyn NativeReadMeter> = concrete.clone();
    let calls = AtomicUsize::new(0);
    concrete.deny_allocations.store(true, Ordering::Release);
    let result = NativeSyncBatch::prepare(2, &meter, |_| {
        calls.fetch_add(1, Ordering::AcqRel);
        Ok(NativeCompletion::ready(Ok(())))
    });
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert_eq!(concrete.memory.load(Ordering::Acquire), 0);
    concrete.deny_allocations.store(false, Ordering::Release);
    let started = Arc::new(AtomicUsize::new(0));
    let result = NativeSyncBatch::prepare(2, &meter, |index| {
        if index == 1 {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        let started = started.clone();
        task_completion(
            move || {
                started.fetch_add(1, Ordering::AcqRel);
                Ok(())
            },
            Some(meter.clone()),
            None,
            None,
            &NATIVE_WORKERS,
        )
    });
    assert!(result.is_err());
    assert_eq!(started.load(Ordering::Acquire), 0);
    assert_eq!(concrete.memory.load(Ordering::Acquire), 0);
    assert!(
        NativeSyncBatch::prepare(NativeSyncBatch::capacity()? + 1, &meter, |_| {
            Err(io::Error::other("oversized batch prepared a member"))
        })
        .is_err()
    );
    Ok(())
}

#[test]
fn batch_full_flushes_keep_original_charges_and_handle_order() -> io::Result<()> {
    initialize_native_io_services()?;
    let concrete = meter();
    let meter: Arc<dyn NativeReadMeter> = concrete.clone();
    let files = files(NativeSyncBatch::capacity()?, &meter)?;
    let before = concrete.used_operations.load(Ordering::Acquire);
    let _inline = InlineBlocking::enter();
    complete(NativeSyncBatch::new(&files, Durability::Full, &meter)?)?;
    assert!(
        inline_blocking_allowed(),
        "batch leaked its execution policy"
    );
    assert_eq!(
        concrete.used_operations.load(Ordering::Acquire) - before,
        files.len() as u64
    );
    for file in &files {
        complete(file.control_async(
            |file, _| {
                assert_eq!(file.metadata()?.len(), 16);
                Ok(())
            },
            Some(meter.clone()),
        )?)?;
    }
    drop(files);
    assert_eq!(concrete.memory.load(Ordering::Acquire), 0);
    Ok(())
}

struct Gate(Arc<(std::sync::Mutex<bool>, Condvar)>);
impl Drop for Gate {
    fn drop(&mut self) {
        *self
            .0
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.0.1.notify_all();
    }
}

#[test]
fn batch_drop_preserves_accepted_jobs_original_custody_and_file_fences() -> io::Result<()> {
    if crate::tests::passed_in_exclusive_process(concat!(
        module_path!(),
        "::batch_drop_preserves_accepted_jobs_original_custody_and_file_fences"
    ))? {
        return Ok(());
    }
    initialize_native_io_services()?;
    let concrete = meter();
    let meter: Arc<dyn NativeReadMeter> = concrete.clone();
    let files = files(2, &meter)?;
    let before = concrete.used_operations.load(Ordering::Acquire);
    let file_bytes = concrete.memory.load(Ordering::Acquire);
    let gate = Gate(Arc::new((std::sync::Mutex::new(false), Condvar::new())));
    let mut batch = NativeSyncBatch::prepare(2, &meter, |index| {
        let gate = gate.0.clone();
        require(files.get(index), "missing physical member")?.control_async(
            move |file, meter| {
                let mut open = gate
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                while !*open {
                    open = gate
                        .1
                        .wait(open)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                sync_file(file, Durability::Full, meter)
            },
            Some(meter.clone()),
        )
    })?;
    let _inline = InlineBlocking::enter();
    assert!(
        Pin::new(&mut batch)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(
        batch
            .pending
            .iter()
            .all(|slot| slot.as_ref().is_some_and(|member| member.pending.is_none())),
        "a member was serialized"
    );
    let states: Vec<_> = batch
        .pending
        .iter()
        .filter_map(|slot| {
            slot.as_ref()
                .and_then(|member| member.state.as_ref())
                .map(Arc::downgrade)
        })
        .collect();
    assert_eq!(states.len(), 2);
    drop(batch);
    assert!(states.iter().all(|state| state.upgrade().is_some()));
    assert!(
        concrete.memory.load(Ordering::Acquire) > file_bytes,
        "physical job backing lost its original custody"
    );
    let mut later: Vec<_> = files
        .iter()
        .map(|file| file.control_async(|_, _| Ok(()), Some(meter.clone())))
        .collect::<io::Result<_>>()?;
    for completion in &mut later {
        assert!(
            Pin::new(&mut *completion)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(
            completion.predecessor.is_some(),
            "cancelled observer released a live file fence"
        );
        assert!(
            completion.pending.is_some(),
            "later control escaped its physical predecessor"
        );
    }
    drop(files);
    drop(gate);
    for completion in later {
        complete(completion)?;
    }
    assert_eq!(concrete.used_operations.load(Ordering::Acquire) - before, 2);
    assert_eq!(concrete.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[cfg(feature = "test-support")]
async fn flush_under_schedule(
    schedule: &crate::testing::Schedule,
    mut events: tokio::sync::mpsc::UnboundedReceiver<crate::testing::Boundary>,
    files: &[NativeFile],
    meter: &Arc<dyn NativeReadMeter>,
) -> io::Result<()> {
    let mut operation = Box::pin(
        schedule.scope(async { NativeSyncBatch::new(files, Durability::Full, meter)?.await }),
    );
    let result = loop {
        tokio::select! {
            biased;
            boundary = events.recv() => require(boundary, "lost simulation boundary")?.release(),
            result = &mut operation => break result,
        }
    };
    drop(operation);
    schedule.drained().await;
    result
}

#[cfg(feature = "test-support")]
#[test]
fn batch_enumerates_every_original_full_flush_fault_boundary() -> io::Result<()> {
    use crate::testing::{DurableFault, InjectedStorageFault, Schedule};
    initialize_native_io_services()?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let width = NativeSyncBatch::capacity()?;
    for cut in 0..=width {
        for fault in [
            DurableFault::Before,
            DurableFault::After,
            DurableFault::Failed,
            DurableFault::DiskFull,
        ] {
            let concrete = meter();
            let meter: Arc<dyn NativeReadMeter> = concrete.clone();
            let files = files(width, &meter)?;
            let (schedule, events) = Schedule::seeded(710);
            if cut < width {
                schedule.cut_at(cut, fault);
            }
            let result = runtime.block_on(flush_under_schedule(&schedule, events, &files, &meter));
            let trace = schedule.durable_writes();
            assert_eq!(trace.len(), (cut + 1).min(width));
            assert!(trace.iter().all(|call| call.operation == "file:sync"));
            if cut == width {
                result?;
            } else {
                let error = require(result.err(), "lost storage fault")?;
                let cause = require(
                    error
                        .get_ref()
                        .and_then(|cause| cause.downcast_ref::<InjectedStorageFault>()),
                    "lost original storage cause",
                )?;
                assert_eq!((cause.ordinal, cause.fault), (cut, fault));
            }
            drop(files);
            assert_eq!(concrete.memory.load(Ordering::Acquire), 0);
        }
    }
    Ok(())
}
