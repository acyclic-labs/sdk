use super::*;

#[derive(Default)]
struct CompletionWake(AtomicUsize);

impl std::task::Wake for CompletionWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Exercise the actual production collection, with every three-caller
/// cancellation order, registered and never-polled observer loss and live peers.
async fn completion_cancellation_orders() -> TestResult {
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        // Each schedule starts with the executor's ordinary cooperative credit.
        tokio::task::yield_now().await;
        let wake = Arc::new(CompletionWake::default());
        let waker = std::task::Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        let mut completing = Completions::default();
        let mut observers = Vec::new();
        for _ in 0..3 {
            let (send, receive) = tokio::sync::oneshot::channel();
            completing.insert(send);
            observers.push(Some(receive));
        }
        assert!(
            completing.poll_cancelled(&mut context).is_pending(),
            "live completion retired before its original idle cut"
        );
        for index in order {
            let before = wake.0.load(Ordering::SeqCst);
            drop(observers[index].take());
            assert!(wake.0.load(Ordering::SeqCst) > before);
            assert!(
                completing.poll_cancelled(&mut context).is_ready(),
                "completion retained its cancelled original observer"
            );
            for observer in observers.iter_mut().flatten() {
                assert!(matches!(
                    observer.try_recv(),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                ));
            }
        }
        assert!(completing.is_empty());
        let (send, receive) = tokio::sync::oneshot::channel();
        drop(receive);
        completing.insert(send);
        assert!(
            completing.is_empty(),
            "never-polled observer retained a completion node"
        );
        let (send, mut receive) = tokio::sync::oneshot::channel();
        completing.insert(send);
        completing.finish(None);
        assert!(matches!(receive.try_recv(), Ok(Ok(()))));
        assert!(completing.is_empty());
    }
    Ok(())
}

/// Completion belongs to the original dispatcher, including discovery, work
/// spilled across cooperative batches, cancellation and failed deliveries.
#[tokio::test]
async fn completion_joins_discovery_delivery_and_producer_lifetime() -> TestResult {
    completion_cancellation_orders().await?;
    for (stop_before_poll, stopped) in [(true, false), (false, false), (false, true)] {
        let provider = Arc::new(Counted::new(MemoryStream::default()));
        let store = Store::new(Arc::clone(&provider), root()?);
        let handler = Arc::new(Gated {
            package: Package::new()?,
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            gate: tokio::sync::Semaphore::new(0),
        });
        let runtime = Runtime::new(store, handler.clone(), Arc::new(SystemClock))
            .with_limits(RuntimeLimits::new(1, 1)?);
        let before = provider.requests();
        let mut unstarted = Box::pin(runtime.complete());
        assert!(
            matches!(
                futures::poll!(unstarted.as_mut()),
                Poll::Ready(Err(Error::CompletionStopped))
            ),
            "completion parked without an original dispatcher producer"
        );
        let calls = provider.requests().since(before);
        assert_eq!(calls.control(), 0);
        assert_eq!(calls.data(), 0);
        let mut serving = Box::pin(runtime.run(|_, error| panic!("unexpected delivery: {error}")));
        let mut duplicate = Box::pin(runtime.run(|_, _| panic!("duplicate dispatcher delivered")));
        assert!(matches!(
            futures::poll!(duplicate.as_mut()),
            Poll::Ready(Err(Error::CompletionStopped))
        ));
        drop(duplicate);
        let mut completed = Box::pin(runtime.complete());
        assert!(futures::poll!(completed.as_mut()).is_pending());
        if stop_before_poll {
            drop(serving);
            assert!(
                matches!(
                    futures::poll!(completed.as_mut()),
                    Poll::Ready(Err(Error::CompletionStopped))
                ),
                "completion producer survived its unpolled host"
            );
            assert!(matches!(
                runtime.complete().await,
                Err(Error::CompletionStopped)
            ));
            continue;
        }
        // The dispatcher has already restored an EMPTY root. Creation must
        // arrive through its original follow, not a second placement path.
        observe_while_running(serving.as_mut(), async {
            completed.as_mut().await.map_err(Into::into)
        })
        .await?;
        let mut addresses = Vec::new();
        for index in 0..4 {
            addresses.push(
                spawn_with(
                    runtime.store(),
                    &format!("complete-{index}"),
                    &[b"emit", b"emit"],
                )
                .await?,
            );
        }
        let mut completed = Box::pin(runtime.complete());
        let mut cancelled = Box::pin(runtime.complete());
        assert!(futures::poll!(completed.as_mut()).is_pending());
        assert!(futures::poll!(cancelled.as_mut()).is_pending());
        drop(cancelled);
        observe_while_running(serving.as_mut(), async {
            state_after(runtime.store(), &addresses[0], |state| state.epoch() == 1).await?;
            Ok(())
        })
        .await?;
        assert_eq!(handler.active.load(Ordering::SeqCst), 1);
        assert!(
            futures::poll!(completed.as_mut()).is_pending(),
            "completion escaped an active handler"
        );
        if stopped {
            drop(serving);
            assert!(matches!(completed.await, Err(Error::CompletionStopped)));
            assert!(matches!(
                runtime.complete().await,
                Err(Error::CompletionStopped)
            ));
            assert_eq!(handler.active.load(Ordering::SeqCst), 0);
            assert!(
                records(provider.as_ref(), "out/complete-0")
                    .await?
                    .is_empty()
            );
            continue;
        }
        handler.gate.add_permits(1);
        observe_while_running(serving.as_mut(), async {
            state_after(runtime.store(), &addresses[0], |state| {
                state.cursor("inbox") == Some(1)
            })
            .await?;
            Ok(())
        })
        .await?;
        assert!(
            futures::poll!(completed.as_mut()).is_pending(),
            "completion escaped a later publication"
        );
        handler.gate.add_permits(7);
        observe_while_running(serving.as_mut(), async {
            completed.await.map_err(Into::into)
        })
        .await?;
        assert_eq!(runtime.counts(), (addresses.len(), 0));
        for index in 0..addresses.len() {
            assert_eq!(
                records(provider.as_ref(), &format!("out/complete-{index}")).await?,
                vec![Bytes::from_static(b"r0"), Bytes::from_static(b"r1")]
            );
        }
        // An idle cut adds exactly ONE tail observation regardless of the
        // resident population, with no state reads, writes or actor scans.
        let before = provider.requests();
        observe_while_running(serving.as_mut(), async {
            runtime.complete().await.map_err(Into::into)
        })
        .await?;
        let calls = provider.requests().since(before);
        assert_eq!(
            calls.control(),
            1,
            "completion added a control request outside its original idle cut"
        );
        assert_eq!(calls.data(), 0);
        // Keep a real actor delivery parked while its accepted completion
        // caller retires. The original handoff credit proves the dispatcher
        // received it; cancellation must wake that SAME dispatcher, not wait
        // for the handler or an idle cut to release resources.
        let cancelled_actor = spawn_with(runtime.store(), "complete-cancelled", &[b"emit"]).await?;
        observe_while_running(serving.as_mut(), async {
            state_after(runtime.store(), &cancelled_actor, |state| {
                state.epoch() == 1
            })
            .await?;
            Ok(())
        })
        .await?;
        let mut cancelled = Box::pin(runtime.complete());
        assert!(futures::poll!(cancelled.as_mut()).is_pending());
        observe_while_running(serving.as_mut(), async {
            let _handoff = runtime.completions.get().unwrap().reserve().await?;
            Ok(())
        })
        .await?;
        let wake = Arc::new(CompletionWake::default());
        let waker = std::task::Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        // Five scheduling classes, one immediate event per cooperative step.
        // This is a bounded schedule over the actual dispatcher, not a recheck.
        for _ in 0..5 {
            assert!(std::future::Future::poll(serving.as_mut(), &mut context).is_pending());
        }
        let before = wake.0.load(Ordering::SeqCst);
        drop(cancelled);
        assert!(
            wake.0.load(Ordering::SeqCst) > before,
            "cancelled completion did not wake the original dispatcher"
        );
        drop(serving);
    }
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    spawn_with(&store, "complete-error", &[b"invalid-command"]).await?;
    let runtime = Runtime::new(store, Arc::new(Package::new()?), Arc::new(SystemClock));
    let mut serving = Box::pin(runtime.run(|_, error| {
        assert!(matches!(error, Error::Handler(_)));
    }));
    let refused =
        observe_while_running(serving.as_mut(), async { Ok(runtime.complete().await) }).await?;
    assert!(
        matches!(refused, Err(Error::CompletionFailed(error)) if matches!(error.as_ref(), Error::Handler(_)))
    );
    for stop_before_start in [false, true] {
        let provider = Arc::new(Counted::new(MemoryStream::default()));
        let target = LocalTarget::start(&provider, Arc::new(Package::new()?), provider.ledger())?;
        let address = actor("completion-startup")?;
        let mut completed = Box::pin(target.complete(&address));
        assert!(
            futures::poll!(completed.as_mut()).is_pending(),
            "completion skipped its original dispatcher startup event"
        );
        if stop_before_start {
            let (_, task, _) = target
                .running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .expect("unstarted original host");
            task.abort();
            assert!(task.await.is_err());
            assert!(matches!(completed.await, Err(Error::CompletionStopped)));
        } else {
            assert_eq!(
                spawn_with(target.store(), "completion-startup", &[b"emit"]).await?,
                address
            );
            completed.await?;
            assert_eq!(
                records(provider.as_ref(), "out/completion-startup").await?,
                vec![Bytes::from_static(b"r0")]
            );
        }
    }
    Ok(())
}
