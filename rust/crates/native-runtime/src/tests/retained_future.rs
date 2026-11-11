use super::*;
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct Probe {
    wake: Mutex<Option<Waker>>,
    polls: AtomicUsize,
    ready: AtomicBool,
    dropped: AtomicBool,
    funded: AtomicU64,
}
struct Pending {
    probe: Arc<Probe>,
    meter: Arc<ReadBudgetMeter>,
}
impl Future for Pending {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let poll = self.probe.polls.fetch_add(1, Ordering::SeqCst);
        *self.probe.wake.lock().unwrap() = Some(cx.waker().clone());
        // Two finite queued wakes exercise the in-poll handoff without any
        // runtime, readiness retry, timer or concurrent task.
        if poll < 2 {
            cx.waker().wake_by_ref();
        }
        if self.probe.ready.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        assert_eq!(
            self.meter.memory.load(Ordering::Acquire),
            self.probe.funded.load(Ordering::Acquire),
            "future backing credit retired before its payload"
        );
        self.probe.dropped.store(true, Ordering::Release);
    }
}

#[test]
fn retained_future_wakes_and_stop_preserve_exact_original_backing() -> io::Result<()> {
    for complete in [false, true] {
        let observed = Arc::new(ReadBudgetMeter::new(0, 0));
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let probe = Arc::new(Probe::default());
        let future = Pending {
            probe: Arc::clone(&probe),
            meter: Arc::clone(&observed),
        };
        let mut admitted = None;
        let created = acyclic_test_support::measure(|| {
            admitted = Some(crate::retain_future(future, &meter));
        });
        let owner = admitted.ok_or(io::ErrorKind::InvalidData)??;
        assert_eq!(created.count_current, 2);
        let funded = observed.memory.load(Ordering::Acquire);
        assert_eq!(
            funded,
            u64::try_from(created.bytes_current).map_err(|_| io::ErrorKind::InvalidData)?
        );
        probe.funded.store(funded, Ordering::Release);
        assert_eq!(probe.polls.load(Ordering::Acquire), 3);
        assert!(!owner.is_stopped());
        if complete {
            probe.ready.store(true, Ordering::Release);
            let wake = probe
                .wake
                .lock()
                .unwrap()
                .take()
                .ok_or(io::ErrorKind::InvalidData)?;
            wake.wake();
            assert_eq!(probe.polls.load(Ordering::Acquire), 4);
        } else {
            observed.cancelled.store(true, Ordering::Release);
        }
        owner.stop()?;
        assert!(owner.is_stopped());
        assert!(probe.dropped.load(Ordering::Acquire));
        assert!(observed.memory.load(Ordering::Acquire) < funded);
        drop(owner);
        // An externally retained event waker still owns the original control
        // allocation. Its last alias, rather than stop, returns that credit.
        let wake = probe.wake.lock().unwrap().take();
        drop(wake);
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn retained_future_refuses_before_poll_and_returns_partial_reservations() {
    for limit in [0, 1] {
        let observed = Arc::new(ReadBudgetMeter {
            allocation_limit: limit,
            ..ReadBudgetMeter::new(0, 0)
        });
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let probe = Arc::new(Probe::default());
        let future = Pending {
            probe: Arc::clone(&probe),
            meter: Arc::clone(&observed),
        };
        let mut result = None;
        let physical = acyclic_test_support::measure(|| {
            result = Some(crate::retain_future(future, &meter));
        });
        assert!(result.unwrap().is_err());
        assert_eq!(physical.count_current, 0);
        assert_eq!(physical.bytes_current, 0);
        assert_eq!(probe.polls.load(Ordering::Acquire), 0);
        assert!(probe.dropped.load(Ordering::Acquire));
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    }
}

#[test]
fn retained_future_panic_retires_the_frame_before_reporting_failure() -> io::Result<()> {
    struct Released(Arc<AtomicBool>);
    impl Drop for Released {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let observed = Arc::new(ReadBudgetMeter::new(0, 0));
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let retired = Arc::new(AtomicBool::new(false));
    let payload = Released(Arc::clone(&retired));
    let owner = crate::retain_future(
        async move {
            let _payload = payload;
            panic!("retained future failed");
        },
        &meter,
    )?;
    assert!(retired.load(Ordering::Acquire));
    assert!(owner.is_stopped());
    assert_eq!(
        owner.stop().err().ok_or(io::ErrorKind::InvalidData)?.kind(),
        io::ErrorKind::Other
    );
    drop(owner);
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn retained_future_self_stop_never_claims_callback_quiescence() -> io::Result<()> {
    struct SelfStop {
        owner: Arc<Mutex<Option<crate::OwnedFuture>>>,
        wake: Arc<Mutex<Option<Waker>>>,
        retired: Arc<AtomicBool>,
    }
    impl Future for SelfStop {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            *self.wake.lock().unwrap() = Some(cx.waker().clone());
            let owner = self.owner.lock().unwrap().take();
            if let Some(owner) = owner {
                assert_eq!(
                    owner.stop().err().unwrap().kind(),
                    io::ErrorKind::WouldBlock
                );
                assert!(!owner.is_stopped(), "the callback is still running");
                drop(owner);
            }
            Poll::Pending
        }
    }
    impl Drop for SelfStop {
        fn drop(&mut self) {
            self.retired.store(true, Ordering::Release);
        }
    }
    let observed = Arc::new(ReadBudgetMeter::new(0, 0));
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let owner = Arc::new(Mutex::new(None));
    let wake = Arc::new(Mutex::new(None));
    let retired = Arc::new(AtomicBool::new(false));
    let future = SelfStop {
        owner: Arc::clone(&owner),
        wake: Arc::clone(&wake),
        retired: Arc::clone(&retired),
    };
    let retained = crate::retain_future(future, &meter)?;
    *owner.lock().unwrap() = Some(retained);
    let pending = wake
        .lock()
        .unwrap()
        .take()
        .ok_or(io::ErrorKind::InvalidData)?;
    pending.wake();
    assert!(retired.load(Ordering::Acquire));
    drop(wake.lock().unwrap().take());
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    Ok(())
}
