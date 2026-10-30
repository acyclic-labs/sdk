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
    let error = hub.poller.failed_wait().unwrap_err();
    let raw = error.raw_os_error();
    assert!(
        !hub.accept(Err(error)),
        "a fatal wait must stop, with zero retries"
    );
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
    hub.fail(io::Error::other(
        "a later failure cannot replace the original",
    ));
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
    for outcome in [armed.block(), timed.block(), hub.arm(&[], None)?.block()] {
        let ProcessWaitOutcome::ServiceFailed(error) = outcome else {
            panic!("lost fatal terminal event: {outcome:?}")
        };
        assert_eq!(error.raw_os_error(), raw);
    }
    assert!(hub.registry().armed.is_empty());
    assert!(hub.registry().timers.is_empty());
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
    drop(hub.poller.failed_wait().unwrap_err());
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
    hub.fail(hub.poller.failed_wait().unwrap_err());
    assert!(matches!(cancelled.block(), ProcessWaitOutcome::Cancelled));
    Ok(())
}
