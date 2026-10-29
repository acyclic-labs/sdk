use super::*;
use std::{num::NonZeroUsize, sync::mpsc, task::Waker};

fn executor(shard: &Shard) -> ActorExecutor {
    ActorExecutor {
        handle: shard.handle.clone(),
        alive: Arc::clone(&shard.alive),
    }
}

fn assert_failure<T>(result: io::Result<T>, expected: ActorFailure) {
    let error = result.err().expect("delivery unexpectedly succeeded");
    assert_eq!(error.get_ref().unwrap().downcast_ref(), Some(&expected));
}

#[test]
fn canonical_placement_and_wakes_never_move_an_actor() -> io::Result<()> {
    let pool = Coordination::start(NonZeroUsize::new(2).unwrap())?;
    pool.block_on(async {
        let original = pool
            .actor(&"actor-reference")
            .unwrap()
            .spawn(async { std::thread::current().id() })
            .await?;
        for count in [1, 100_000] {
            let measured = acyclic_test_support::measure(|| {
                for _ in 0..count {
                    let selected = pool.actor(&"actor-reference").unwrap();
                    assert_eq!(
                        selected.handle.id(),
                        pool.actor(&"actor-reference").unwrap().handle.id()
                    );
                }
            });
            assert_eq!(measured.count_total, 0);
            // Retaining an additional heap owner on selection violates the
            // same zero-allocation floor, at either population size.
            let allocated = acyclic_test_support::measure(|| {
                for _ in 0..count {
                    std::hint::black_box(Box::new(pool.actor(&"actor-reference").unwrap()));
                }
            });
            assert_eq!(allocated.count_total, count);
        }
        let (producer, event) = tokio::sync::oneshot::channel();
        let (armed, listening) = tokio::sync::oneshot::channel();
        let task = pool.actor(&"actor-reference").unwrap().spawn(async move {
            let before = std::thread::current().id();
            let _ = armed.send(());
            event.await.map_err(io::Error::other)?;
            let after = std::thread::current().id();
            Ok::<_, io::Error>((before, after))
        });
        listening.await.map_err(io::Error::other)?;
        producer.send(()).map_err(|()| io::ErrorKind::BrokenPipe)?;
        assert_eq!(task.await??, (original, original));
        // A rotating executor fails the exact same identity oracle.
        let other = executor(&pool.others[0])
            .spawn(async { std::thread::current().id() })
            .await?;
        let first = executor(&pool.first)
            .spawn(async { std::thread::current().id() })
            .await?;
        assert_ne!((first, other), (original, original));
        Ok(())
    })
}

async fn sibling_progress(first: ActorExecutor, sibling: ActorExecutor) -> io::Result<()> {
    let producer_thread = sibling.spawn(async { std::thread::current().id() }).await?;
    let (starting, started) = tokio::sync::oneshot::channel();
    let (release, held) = mpsc::channel();
    let busy = first.spawn(async move {
        // Finite oracle: a synchronous wait whose sole producer is on this
        // same thread cannot progress. Refuse BEFORE entering that wait.
        if std::thread::current().id() == producer_thread {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        let _ = starting.send(());
        held.recv().map_err(io::Error::other)
    });
    if started.await.is_err() {
        return busy.await?;
    }
    // This is the one actual producer of the occupied shard's event. It
    // executes while that shard is synchronously blocked, without a timer.
    sibling
        .spawn(async move { release.send(()).map_err(io::Error::other) })
        .await??;
    busy.await?
}

#[test]
fn a_busy_shard_cannot_delay_its_sibling_producer() -> io::Result<()> {
    let pool = Coordination::start(NonZeroUsize::new(2).unwrap())?;
    pool.block_on(sibling_progress(
        executor(&pool.first),
        executor(&pool.others[0]),
    ))?;
    // The actual single-current-thread scheduling boundary fails the same
    // progress oracle; the old owner cannot run its sibling producer.
    let baseline = Coordination::start(NonZeroUsize::new(1).unwrap())?;
    assert_eq!(
        baseline
            .block_on(sibling_progress(
                executor(&baseline.first),
                executor(&baseline.first)
            ))
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    Ok(())
}

#[test]
fn delivery_custody_outlives_its_observer_and_producer_loss_is_terminal() -> io::Result<()> {
    let pool = Coordination::start(NonZeroUsize::new(1).unwrap())?;
    let owner = pool.actor(&"actor").unwrap();
    pool.block_on(async {
        let (started, starting) = tokio::sync::oneshot::channel();
        let (release, held) = tokio::sync::oneshot::channel::<()>();
        let (finished, finishing) = tokio::sync::oneshot::channel();
        let accepted = owner.spawn(async move {
            let _ = started.send(());
            let result = held.await;
            let _ = finished.send(result.is_err());
        });
        drop(accepted);
        starting.await.map_err(io::Error::other)?;
        drop(release);
        assert!(finishing.await.map_err(io::Error::other)?);
        let aborted = owner.spawn(std::future::pending::<()>());
        aborted.abort();
        assert_failure(aborted.await, ActorFailure::Cancelled);
        let panicked = owner.spawn(async { panic!("actor panic control") });
        assert_failure(panicked.await, ActorFailure::Panicked);
        Ok::<_, io::Error>(())
    })?;
    let task = owner.spawn(std::future::pending::<()>());
    drop(pool);
    // The shutdown producer is owned by that exact pool; losing it retires
    // its runtime and completes every original task, including future work.
    let observer = tokio::runtime::Builder::new_current_thread().build()?;
    observer.block_on(async {
        assert_failure(task.await, ActorFailure::Unavailable);
        let future = owner.spawn(async { panic!("closed shard ran application code") });
        let mut future = std::pin::pin!(future);
        assert!(matches!(future.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Err(error))
            if error.get_ref().unwrap().downcast_ref() == Some(&ActorFailure::Unavailable)));
    });
    Ok(())
}

#[test]
fn retirement_wakes_observers_while_their_physical_shard_is_blocked() -> io::Result<()> {
    struct Wakes(std::sync::atomic::AtomicUsize);
    impl std::task::Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let pool = Coordination::start(NonZeroUsize::new(1).unwrap())?;
    let owner = pool.actor(&"blocked-actor")?;
    let (release, held) = mpsc::channel::<()>();
    let (starting, started) = tokio::sync::oneshot::channel();
    let (retired, retirement) = tokio::sync::oneshot::channel();
    let busy = owner.spawn(async move {
        let _ = starting.send(());
        let result = held.recv();
        let _ = retired.send(());
        result
    });
    pool.block_on(started).map_err(io::Error::other)?;
    let queued = owner.spawn(std::future::pending::<()>());
    let mut former = crate::task::spawn_on(&owner.handle, std::future::pending::<()>());
    let observer = tokio::runtime::Builder::new_current_thread().build()?;
    observer.block_on(async {
        let mut busy = std::pin::pin!(busy);
        let mut queued = std::pin::pin!(queued);
        let wakes = Arc::new(Wakes(std::sync::atomic::AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));
        let mut context = Context::from_waker(&waker);
        assert!(busy.as_mut().poll(&mut context).is_pending());
        assert!(queued.as_mut().poll(&mut context).is_pending());
        drop(pool);
        // These observers must settle before the only physical release event.
        // A plain JoinHandle remains pending here: its producer cannot poll.
        assert_eq!(wakes.0.load(std::sync::atomic::Ordering::Relaxed), 2);
        let Poll::Ready(busy) = busy.as_mut().poll(&mut context) else {
            panic!("retired busy observer retained a blocked producer")
        };
        let Poll::Ready(queued) = queued.as_mut().poll(&mut context) else {
            panic!("retired queued observer retained a blocked producer")
        };
        assert_failure(busy, ActorFailure::Unavailable);
        assert_failure(queued, ActorFailure::Unavailable);
        assert!(Pin::new(&mut former).poll(&mut context).is_pending());
        drop(release);
        retirement.await.map_err(io::Error::other)
    })
}

#[test]
fn application_lifetime_joins_original_blocked_delivery_after_observer_loss() -> io::Result<()> {
    struct Original {
        retired: Arc<std::sync::atomic::AtomicBool>,
        finished: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl Drop for Original {
        fn drop(&mut self) {
            self.retired
                .store(true, std::sync::atomic::Ordering::Release);
            if let Some(finished) = self.finished.take() {
                let _ = finished.send(());
            }
        }
    }
    let pool = Coordination::start(NonZeroUsize::new(1).unwrap())?;
    for retained in [true, false] {
        let executor = pool.actor(&"application-owned-actor")?;
        let (alive, mut lifetime) = tokio::sync::watch::channel(());
        let (starting, started) = tokio::sync::oneshot::channel();
        let (release, held) = mpsc::channel::<()>();
        let (finished, finishing) = tokio::sync::oneshot::channel();
        let retired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let original = Original {
            retired: Arc::clone(&retired),
            finished: Some(finished),
        };
        let submit = async move {
            executor.spawn(async move {
                let _original = original;
                let _ = starting.send(());
                held.recv().map_err(io::Error::other)
            })
        };
        let task = if retained {
            pool.block_on(with_actor_lifetime(alive, submit))
        } else {
            // The former serving-loop owner closes its original lifetime
            // before accepted work on another thread can physically retire.
            let task = pool.block_on(submit);
            drop(alive);
            task
        };
        pool.block_on(started).map_err(io::Error::other)?;
        task.abort();
        drop(task);
        assert!(!retired.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(lifetime.has_changed().is_ok(), retained);
        // This is the sole producer of the blocked physical wait. Losing it
        // returns a typed channel error; application closure follows Drop.
        drop(release);
        pool.block_on(finishing).map_err(io::Error::other)?;
        assert!(pool.block_on(lifetime.changed()).is_err());
        assert!(retired.load(std::sync::atomic::Ordering::Acquire));
    }
    Ok(())
}

#[test]
fn actor_lifetime_inheritance_is_constant_and_excludes_background_tasks() -> io::Result<()> {
    let pool = Coordination::start(NonZeroUsize::new(1).unwrap())?;
    let (alive, lifetime) = tokio::sync::watch::channel(());
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    let mut scoped = None;
    let allocation = acyclic_test_support::measure(|| {
        scoped = Some(with_actor_lifetime(alive, async {
            for count in [1, 100_000] {
                let inherited = acyclic_test_support::measure(|| {
                    for _ in 0..count {
                        drop(std::hint::black_box(lifetime::inherit(
                            std::future::pending::<()>(),
                        )));
                    }
                });
                assert_eq!(inherited.count_total, 0);
                let copied = acyclic_test_support::measure(|| {
                    for _ in 0..count {
                        drop(std::hint::black_box(Box::new(lifetime::inherit(
                            std::future::pending::<()>(),
                        ))));
                    }
                });
                assert_eq!(copied.count_total, count);
            }
            crate::spawn(async move { held.await })
        }));
    });
    assert_eq!(allocation.count_total, 1);
    let background = pool.block_on(scoped.ok_or(io::ErrorKind::NotFound)?);
    assert!(
        lifetime.has_changed().is_err(),
        "a transport cannot retain its own close producer"
    );
    drop(release);
    assert!(pool.block_on(background)?.is_err());
    Ok(())
}

#[test]
fn nested_actor_submission_retains_the_original_application_producer() -> io::Result<()> {
    let pool = Coordination::start(NonZeroUsize::new(1).unwrap())?;
    let parent = pool.actor(&"parent")?;
    let child = pool.actor(&"child")?;
    let (alive, mut lifetime) = tokio::sync::watch::channel(());
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    let task = pool.block_on(with_actor_lifetime(alive, async move {
        parent
            .spawn(async move { child.spawn(async move { held.await }) })
            .await
    }))?;
    assert!(lifetime.has_changed().is_ok());
    drop(task);
    drop(release);
    assert!(pool.block_on(lifetime.changed()).is_err());
    Ok(())
}

#[cfg(feature = "test-support")]
#[test]
fn seeded_task_gates_preserve_the_original_physical_shard() -> io::Result<()> {
    let pool = Coordination::start(NonZeroUsize::new(1).unwrap())?;
    pool.block_on(async {
        let expected = pool
            .actor(&"actor")
            .unwrap()
            .spawn(async { std::thread::current().id() })
            .await?;
        let (schedule, mut events) = crate::testing::Schedule::seeded(71);
        let run = schedule.scope(async {
            let identity = pool
                .actor(&"actor")
                .unwrap()
                .spawn(async { std::thread::current().id() })
                .await?;
            schedule.drained().await;
            Ok::<_, io::Error>(identity)
        });
        tokio::pin!(run);
        loop {
            tokio::select! {
                event = events.recv() => event.ok_or(io::ErrorKind::BrokenPipe)?.release(),
                identity = &mut run => { assert_eq!(identity?, expected); return Ok(()); }
            }
        }
    })
}
