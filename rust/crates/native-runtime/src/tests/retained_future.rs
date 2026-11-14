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

#[test]
fn retained_future_self_wakes_yield_the_calling_thread() -> io::Result<()> {
    crate::initialize_native_io_services()?;
    let caller = std::thread::current().id();
    let reentered = Arc::new(AtomicBool::new(false));
    let observed = reentered.clone();
    let (completed, completion) = std::sync::mpsc::channel();
    let mut polls = 0;
    let future = std::future::poll_fn(move |cx| {
        polls += 1;
        if polls > 1 && std::thread::current().id() == caller {
            observed.store(true, Ordering::Release);
        }
        if polls == 4 {
            completed.send(()).unwrap();
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    });
    let meter: Arc<dyn NativeReadMeter> = Arc::new(ReadBudgetMeter::new(0, 0));
    let owner = crate::retain_future(future, &meter)?;
    completion.recv().map_err(io::Error::other)?;
    owner.stop()?;
    assert!(
        !reentered.load(Ordering::Acquire),
        "self-wakes monopolized the caller"
    );
    assert!(owner.is_stopped());
    Ok(())
}
impl Future for Pending {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.probe.polls.fetch_add(1, Ordering::SeqCst);
        *self.probe.wake.lock().unwrap() = Some(cx.waker().clone());
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
    crate::initialize_native_io_services()?;
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
        assert_eq!(created.count_current, 3);
        let funded = observed.memory.load(Ordering::Acquire);
        assert_eq!(
            funded,
            u64::try_from(created.bytes_current).map_err(|_| io::ErrorKind::InvalidData)?
        );
        probe.funded.store(funded, Ordering::Release);
        assert_eq!(probe.polls.load(Ordering::Acquire), 1);
        assert!(!owner.is_stopped());
        if complete {
            probe.ready.store(true, Ordering::Release);
            let wake = probe
                .wake
                .lock()
                .map_err(|_| io::Error::other("poisoned wake"))?
                .take()
                .ok_or(io::ErrorKind::InvalidData)?;
            wake.wake();
            assert_eq!(probe.polls.load(Ordering::Acquire), 2);
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
        let wake = probe
            .wake
            .lock()
            .map_err(|_| io::Error::other("poisoned wake"))?
            .take();
        drop(wake);
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn retained_future_refuses_before_poll_and_returns_partial_reservations() {
    crate::initialize_native_io_services().unwrap();
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
    crate::initialize_native_io_services()?;
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
    crate::initialize_native_io_services()?;
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
    *owner
        .lock()
        .map_err(|_| io::Error::other("poisoned owner"))? = Some(retained);
    let pending = wake
        .lock()
        .map_err(|_| io::Error::other("poisoned wake"))?
        .take()
        .ok_or(io::ErrorKind::InvalidData)?;
    pending.wake();
    assert!(retired.load(Ordering::Acquire));
    drop(
        wake.lock()
            .map_err(|_| io::Error::other("poisoned wake"))?
            .take(),
    );
    assert_eq!(observed.memory.load(Ordering::Acquire), 0);
    Ok(())
}

#[cfg(feature = "test-support")]
#[test]
fn retained_future_foreign_wake_keeps_original_scope_and_restores_the_caller() -> io::Result<()> {
    crate::initialize_native_io_services()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (expected, _) = crate::testing::Schedule::seeded(307);
        let expected = expected
            .scope(async {
                let mut bytes = [0; 32];
                assert!(crate::testing::fill_seeded(&mut bytes));
                bytes
            })
            .await;
        let (original, _) = crate::testing::Schedule::seeded(307);
        let (foreign, _) = crate::testing::Schedule::seeded(509);
        let observed = Arc::new(ReadBudgetMeter::new(0, 0));
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let records = Arc::new(Mutex::new(Vec::with_capacity(2)));
        let wake = Arc::new(Mutex::new(None::<Waker>));
        let output = Arc::clone(&records);
        let pending = Arc::clone(&wake);
        let future = std::future::poll_fn(move |cx| {
            let mut bytes = [0; 16];
            assert!(crate::testing::fill_seeded(&mut bytes));
            let mut records = output.lock().unwrap();
            records.push(bytes);
            *pending.lock().unwrap() = Some(cx.waker().clone());
            if records.len() == 2 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });
        let owner = original
            .scope(async { crate::retain_future(future, &meter) })
            .await?;
        let foreign_observed = foreign
            .scope(async {
                let pending = wake.lock().unwrap().take().unwrap();
                pending.wake();
                let mut bytes = [0; 16];
                assert!(crate::testing::fill_seeded(&mut bytes));
                bytes
            })
            .await;
        let (foreign_expected, _) = crate::testing::Schedule::seeded(509);
        let foreign_expected = foreign_expected
            .scope(async {
                let mut bytes = [0; 16];
                assert!(crate::testing::fill_seeded(&mut bytes));
                bytes
            })
            .await;
        assert_eq!(foreign_observed, foreign_expected);
        {
            let records = records
                .lock()
                .map_err(|_| io::Error::other("poisoned records"))?;
            let [first, second] = records.as_slice() else {
                return Err(io::Error::other("expected two retained records"));
            };
            assert_eq!(first, &expected[..16]);
            assert_eq!(second, &expected[16..]);
        }
        assert!(owner.is_stopped());
        owner.stop()?;
        drop(owner);
        drop(
            wake.lock()
                .map_err(|_| io::Error::other("poisoned wake"))?
                .take(),
        );
        original.drained().await;
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
        Ok(())
    })
}
