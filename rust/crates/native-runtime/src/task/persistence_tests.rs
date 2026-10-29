use super::*;
use std::{future::Future, io, pin::pin, sync::Arc, task::Context};

fn refused(result: std::task::Poll<io::Result<()>>) -> bool {
    let std::task::Poll::Ready(Err(error)) = result else {
        return false;
    };
    error.kind() == io::ErrorKind::WouldBlock
        && error
            .get_ref()
            .is_some_and(|cause| cause.is::<PersistenceRecursion>())
}

#[test]
fn recursive_refusal_drops_inputs_without_a_runtime_or_job_allocation() {
    struct Input(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for Input {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _scope = PersistenceScope::enter();
    for count in [1, 100_000] {
        let allocation = acyclic_test_support::measure(|| {
            for _ in 0..count {
                let input = Input(Arc::clone(&dropped));
                let result = spawn_persistence(move || {
                    drop(input);
                    panic!("a refused persistence closure executed");
                });
                drop(result);
            }
        });
        assert_eq!(allocation.count_total, 0, "{allocation:?}");
    }
    assert_eq!(dropped.load(std::sync::atomic::Ordering::Relaxed), 100_001);
    let result = pin!(spawn_persistence(|| ()));
    assert!(refused(
        result.poll(&mut Context::from_waker(std::task::Waker::noop()))
    ));
    // Bypassing the guard allocates a real job and owned task, violating the
    // same zero-allocation admission oracle even while this scope is active.
    let runtime = crate::coordination().unwrap();
    runtime.block_on(async {
        let mut accepted = None;
        let allocation = acyclic_test_support::measure(|| {
            accepted = Some(submit_persistence(|| ()));
        });
        assert!(allocation.count_total > 0);
        accepted.unwrap().await.unwrap().unwrap();
    });
}

#[test]
fn persistence_scope_restores_thread_state_after_unwinding() {
    assert!(!IN_PERSISTENCE.get());
    let panic = std::panic::catch_unwind(|| {
        let _scope = PersistenceScope::enter();
        assert!(IN_PERSISTENCE.get());
        panic!("storage closure panic");
    });
    assert!(panic.is_err());
    assert!(!IN_PERSISTENCE.get());
}

#[test]
fn moving_an_accepted_wait_into_persistence_refuses_without_releasing_commit_custody() {
    crate::coordination().unwrap().block_on(async {
        let key = Arc::new(std::sync::Mutex::new(()));
        let owned_key = Arc::clone(&key);
        let (started, starting) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let (finished, finishing) = tokio::sync::oneshot::channel();
        let mut accepted = Box::pin(spawn_persistence(move || {
            let guard = owned_key.lock().unwrap();
            let _ = started.send(());
            let result = held.recv();
            drop(guard);
            let _ = finished.send(result);
        }));
        starting.await.unwrap();
        assert!(
            accepted
                .as_mut()
                .poll(&mut Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        let rejected = spawn_persistence(move || {
            refused(
                accepted
                    .as_mut()
                    .poll(&mut Context::from_waker(std::task::Waker::noop())),
            )
        })
        .await
        .unwrap();
        assert!(rejected);
        assert!(key.try_lock().is_err());
        // Dropping the event producer ends the blocked commit with an error;
        // the refused observer has no authority over its physical custody.
        drop(release);
        assert!(finishing.await.unwrap().is_err());
        assert!(key.try_lock().is_ok());
    });
}

async fn saturated_recursion(checked: bool, threads: usize) -> io::Result<usize> {
    let mut jobs = Vec::new();
    let mut starters = Vec::new();
    let mut advances = Vec::new();
    let mut releases = Vec::new();
    let mut observations = Vec::new();
    let mut completions = Vec::new();
    for _ in 0..threads {
        let (started, starting) = tokio::sync::oneshot::channel();
        let (advance, advancing) = std::sync::mpsc::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let (observed, observing) = tokio::sync::oneshot::channel();
        let (finished, finishing) = tokio::sync::oneshot::channel();
        let runtime = tokio::runtime::Handle::current();
        jobs.push(spawn_persistence(move || -> io::Result<()> {
            let _ = started.send(());
            advancing.recv().map_err(io::Error::other)?;
            let _runtime = runtime.enter();
            let work = move || {
                let _ = finished.send(());
            };
            let mut nested = pin!(async move {
                if checked {
                    spawn_persistence(work).await
                } else {
                    submit_persistence(work).await?
                }
            });
            let result = nested
                .as_mut()
                .poll(&mut Context::from_waker(std::task::Waker::noop()));
            let pending = result.is_pending();
            let _ = observed.send((refused(result), pending));
            // Every worker remains occupied until the coordination owner has
            // observed every nested result. No nested producer can run early.
            held.recv().map_err(io::Error::other)?;
            Ok(())
        }));
        starters.push(starting);
        advances.push(advance);
        releases.push(release);
        observations.push(observing);
        completions.push(finishing);
    }
    for starting in starters {
        starting.await.map_err(io::Error::other)?;
    }
    for advance in advances {
        advance.send(()).map_err(io::Error::other)?;
    }
    let mut refusals = 0;
    let mut pending = 0;
    for observing in observations {
        let (refusal, waiting) = observing.await.map_err(io::Error::other)?;
        refusals += usize::from(refusal);
        pending += usize::from(waiting);
    }
    for release in releases {
        release.send(()).map_err(io::Error::other)?;
    }
    for job in jobs {
        job.await??;
    }
    for finishing in completions {
        assert_eq!(finishing.await.is_err(), checked);
    }
    assert_eq!(pending, if checked { 0 } else { threads });
    Ok(refusals)
}

#[test]
fn recursive_saturated_persistence_has_a_finite_progress_oracle() -> io::Result<()> {
    if crate::tests::passed_in_exclusive_process(
        "task::persistence_tests::recursive_saturated_persistence_has_a_finite_progress_oracle",
    )? {
        return Ok(());
    }
    let threads = crate::workers::PERSISTENCE_WORKERS
        .as_ref()
        .map_err(|error| io::Error::other(error.to_string()))?
        .exclusive_threads();
    crate::coordination()?.block_on(async {
        assert_eq!(saturated_recursion(true, threads).await?, threads);
        // Negative control: the exact same saturated service with the guard
        // bypassed yields Pending in every worker. Waiting synchronously here
        // would leave every producer queued behind its own blocked consumer.
        assert_eq!(saturated_recursion(false, threads).await?, 0);
        Ok(())
    })
}

#[cfg(feature = "test-support")]
#[test]
fn seeded_persistence_keeps_physical_custody_while_coordination_cancels() -> io::Result<()> {
    let (schedule, mut boundaries) = crate::testing::Schedule::seeded(71);
    crate::coordination()?.block_on(async {
        let mut operation = Box::pin(schedule.scope(async {
            let key = Arc::new(std::sync::Mutex::new(()));
            let owned_key = Arc::clone(&key);
            let (started, starting) = tokio::sync::oneshot::channel();
            let (release, held) = tokio::sync::oneshot::channel::<()>();
            let (finished, finishing) = tokio::sync::oneshot::channel();
            let observer = spawn_persistence(move || {
                let guard = owned_key.lock().unwrap();
                let _ = started.send(());
                let result = crate::testing::blocking_on(held);
                drop(guard);
                let _ = finished.send(result);
            });
            starting.await.map_err(io::Error::other)?;
            drop(observer);
            // This sibling's progress is the primitive event that releases the
            // blocked commit. Cancellation cannot release its held key early.
            assert!(key.try_lock().is_err());
            drop(release);
            assert!(finishing.await.map_err(io::Error::other)?.is_err());
            assert!(key.try_lock().is_ok());
            Ok::<(), io::Error>(())
        }));
        let result = loop {
            tokio::select! {
                boundary = boundaries.recv() => boundary.ok_or(io::ErrorKind::BrokenPipe)?.release(),
                result = &mut operation => break result,
            }
        };
        result?;
        let mut draining = pin!(schedule.drained());
        loop {
            tokio::select! {
                boundary = boundaries.recv() => boundary.ok_or(io::ErrorKind::BrokenPipe)?.release(),
                () = &mut draining => break,
            }
        }
        Ok(())
    })
}
