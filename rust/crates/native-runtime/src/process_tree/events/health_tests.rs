//! Finite progress oracles drive the real registry and terminal signal. No
//! wall-clock timeout stands in for a producer event.
use super::*;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn isolated_hub() -> io::Result<&'static Hub> {
    Ok(Box::leak(Box::new(Hub {
        poller: platform::Poller::new()?,
        registry: Mutex::new(Registry::default()),
    })))
}

fn source(wake: &crate::ProcessWake) -> Source<'_> {
    #[cfg(unix)]
    {
        use std::os::fd::AsFd;
        Source::Readable(wake.as_fd())
    }
    #[cfg(windows)]
    // SAFETY: the returned source borrows the original wake's owned handle.
    {
        Source::Exit(unsafe { std::os::windows::io::BorrowedHandle::borrow_raw(wake.raw()) })
    }
}

/// Capture the platform's actual fatal wait error, rather than a handwritten
/// fixture or guessed OS error number.
fn fatal_wait_error() -> io::Error {
    #[cfg(target_os = "linux")]
    {
        let mut event = libc::epoll_event { events: 0, u64: 0 };
        // SAFETY: writable output; -1 deliberately names no epoll descriptor.
        assert_eq!(unsafe { libc::epoll_wait(-1, &raw mut event, 1, 0) }, -1);
    }
    #[cfg(target_vendor = "apple")]
    // SAFETY: no buffers; -1 deliberately names no kqueue descriptor.
    assert_eq!(
        unsafe {
            libc::kevent(
                -1,
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        },
        -1
    );
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::IO::{GetQueuedCompletionStatusEx, OVERLAPPED_ENTRY};
        // SAFETY: plain fields, initialized before being passed as output.
        let mut entry: OVERLAPPED_ENTRY = unsafe { std::mem::zeroed() };
        let mut removed = 0;
        // SAFETY: valid output storage; the handle deliberately names no port.
        assert_eq!(
            unsafe {
                GetQueuedCompletionStatusEx(
                    windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE,
                    &raw mut entry,
                    1,
                    &raw mut removed,
                    0,
                    0,
                )
            },
            0
        );
    }
    io::Error::last_os_error()
}

#[test]
fn fatal_error_settles_armed_and_future_waits_without_retry() -> io::Result<()> {
    let hub = isolated_hub()?;
    let wake = crate::ProcessWake::new()?;
    let mut armed = hub.arm(&[source(&wake)], None)?;
    let timed = hub.arm(&[], Some(Instant::now() + Duration::from_secs(3600)))?;
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    assert!(Pin::new(&mut armed).poll(&mut context).is_pending());
    let error = fatal_wait_error();
    let raw = error.raw_os_error();
    assert!(
        !hub.accept(Err(error)),
        "a fatal wait must stop, with zero retries"
    );
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
    for outcome in [armed.block(), timed.block(), hub.arm(&[], None)?.block()] {
        let ProcessWaitOutcome::ServiceFailed(error) = outcome else {
            panic!("lost fatal terminal event: {outcome:?}")
        };
        assert_eq!(error.raw_os_error(), raw);
    }
    assert!(hub.registry().armed.is_empty());
    assert!(hub.registry().timers.is_empty());
    assert!(hub.registry().sources.is_empty());
    Ok(())
}

#[test]
fn ignored_fatal_error_is_rejected_by_the_same_progress_oracle() -> io::Result<()> {
    let hub = isolated_hub()?;
    let wake = crate::ProcessWake::new()?;
    let mut armed = hub.arm(&[source(&wake)], None)?;
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    assert!(Pin::new(&mut armed).poll(&mut context).is_pending());
    // Previous behavior: ignore the poll failure and try another wait. The
    // finite oracle rejects it immediately, without running a hanging wait.
    drop(fatal_wait_error());
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    assert!(Pin::new(&mut armed).poll(&mut context).is_pending());
    drop(Producer(hub));
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
    assert!(matches!(
        armed.block(),
        ProcessWaitOutcome::ServiceFailed(_)
    ));
    Ok(())
}

#[test]
fn interruption_preserves_interest_and_terminal_delivery_is_first_wins() -> io::Result<()> {
    let hub = isolated_hub()?;
    let wake = crate::ProcessWake::new()?;
    let ready = hub.arm(&[source(&wake)], None)?;
    // Synthetic EINTR is a state-machine input, not a captured OS fixture.
    assert!(hub.accept(Err(io::ErrorKind::Interrupted.into())));
    assert!(!ready.fired());
    hub.dispatch(&mut vec![ready.token, ready.token], Instant::now());
    ready.cancel();
    assert!(matches!(ready.block(), ProcessWaitOutcome::Ready));
    let cancelled = hub.arm(&[source(&wake)], None)?;
    cancelled.cancel();
    hub.fail(fatal_wait_error());
    assert!(matches!(cancelled.block(), ProcessWaitOutcome::Cancelled));
    Ok(())
}

#[test]
fn overlapping_interest_is_refused_and_drop_allows_rearm() -> io::Result<()> {
    let wake = crate::ProcessWake::new()?;
    let sibling = crate::ProcessWake::new()?;
    let wait = arm(&[source(&wake)], None)?;
    assert!(
        matches!(arm(&[source(&wake)], None), Err(error) if error.kind() == io::ErrorKind::AlreadyExists)
    );
    assert!(
        matches!(arm(&[source(&sibling), source(&wake)], None), Err(error) if error.kind() == io::ErrorKind::AlreadyExists)
    );
    // Refusal rolls back the entire prefix, leaving unrelated producers live.
    let unrelated = arm(&[source(&sibling)], None)?;
    sibling.notify()?;
    assert!(matches!(unrelated.block(), ProcessWaitOutcome::Ready));
    drop(wait);
    let wait = arm(&[source(&wake)], None)?;
    wake.notify()?;
    assert!(matches!(wait.block(), ProcessWaitOutcome::Ready));
    // A wait with no source and no wake time has no possible producer.
    assert!(matches!(arm(&[], None), Err(error) if error.kind() == io::ErrorKind::InvalidInput));
    Ok(())
}

#[test]
fn arm_ready_drop_costs_at_one_and_one_hundred_thousand_waits() -> io::Result<()> {
    let locks = |hub: &Hub| hub.registry.lock().unwrap().lock_count;
    let mut ready_allocation = None;
    for count in [1, 100_000] {
        let hub = isolated_hub()?;
        let deadline = Instant::now() + Duration::from_secs(3600);
        let start = Instant::now();
        let before = locks(hub);
        let mut waits = Ok(Vec::new());
        let allocations = acyclic_test_support::measure(|| {
            waits = (0..count)
                .map(|_| hub.arm(&[], Some(deadline)))
                .collect::<io::Result<Vec<_>>>();
        });
        let waits = waits?;
        let arm_time = start.elapsed();
        let arm_locks = locks(hub) - before;
        assert_eq!(arm_locks, count);
        let token = waits.get(count / 2).unwrap().token;
        let start = Instant::now();
        let before = locks(hub);
        let mut ready = vec![token];
        let delivered = acyclic_test_support::measure(|| hub.dispatch(&mut ready, Instant::now()));
        let ready_time = start.elapsed();
        let ready_locks = locks(hub) - before;
        assert_eq!(ready_locks, 1);
        let cost = (delivered.count_total, delivered.bytes_total);
        assert_eq!(*ready_allocation.get_or_insert(cost), cost);
        assert_eq!(hub.registry().armed.len(), count - 1);
        assert_eq!(hub.registry().timers.len(), count - 1);
        if count > 1 {
            // Negative control: a snapshot of unrelated registrations fails
            // the same fixed-allocation oracle used for one ready token.
            let snapshot = acyclic_test_support::measure(|| {
                std::hint::black_box(hub.registry().armed.keys().copied().collect::<Vec<_>>());
            });
            assert!(snapshot.bytes_total > delivered.bytes_total);
        }
        let start = Instant::now();
        let before = locks(hub);
        drop(waits);
        let drop_time = start.elapsed();
        let drop_locks = locks(hub) - before;
        assert_eq!(drop_locks, count);
        assert!(hub.registry().armed.is_empty());
        assert!(hub.registry().timers.is_empty());
        eprintln!(
            "waits={count} arm={arm_time:?} ready-one={ready_time:?} drop={drop_time:?} locks=({arm_locks},{ready_locks},{drop_locks}) allocations={allocations:?} delivered={delivered:?}"
        );
    }
    Ok(())
}
