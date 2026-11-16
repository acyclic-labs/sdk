use super::*;

struct HeldWake {
    entered: mpsc::Sender<()>,
    released: std::sync::Mutex<mpsc::Receiver<()>>,
}

impl HeldWake {
    fn hold(&self) -> io::Result<()> {
        self.entered.send(()).map_err(io::Error::other)?;
        self.released
            .lock()
            .map_err(|_| io::Error::other("release lock poisoned"))?
            .recv()
            .map_err(io::Error::other)
    }
}

impl std::task::Wake for HeldWake {
    fn wake(self: Arc<Self>) {
        assert!(self.hold().is_ok(), "wake synchronization failed");
    }
}

#[tokio::test]
async fn cold_service_retains_owner_through_body_and_terminal_wake() -> io::Result<()> {
    static POOLS: [LazyLock<io::Result<NativeWorkers>>; 2] = [
        LazyLock::new(|| start_workers("demand-body-proof", 2)),
        LazyLock::new(|| start_workers("demand-wake-proof", 2)),
    ];
    for (pool, terminal) in POOLS.iter().zip([false, true]) {
        let (entered, entering) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let held = Arc::new(HeldWake {
            entered,
            released: std::sync::Mutex::new(released),
        });
        let waker = if terminal {
            std::task::Waker::from(held.clone())
        } else {
            std::task::Waker::noop().clone()
        };
        // Register the original terminal wake before allowing the body to end.
        let (finish, finishing) = mpsc::channel();
        let first = crate::task_completion(
            move || {
                finishing.recv().map_err(io::Error::other)?;
                if !terminal {
                    held.hold()?;
                }
                Ok(())
            },
            None,
            None,
            None,
            pool,
        )?;
        let mut first = std::pin::pin!(first);
        assert!(
            first
                .as_mut()
                .poll(&mut std::task::Context::from_waker(&waker))
                .is_pending()
        );
        finish.send(()).map_err(io::Error::other)?;
        entering.recv().map_err(io::Error::other)?;
        {
            let admission = workers(pool, false)?
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(
                admission.started_workers, 1,
                "cold work started unused owners"
            );
            assert_eq!(
                admission.running_jobs, 1,
                "blocked work lost its physical owner"
            );
        }
        let second = crate::task_completion(|| Ok(7), None, None, None, pool)?;
        assert_eq!(second.await?, 7);
        release.send(()).map_err(io::Error::other)?;
        first.await?;
    }
    Ok(())
}

/// A seeded world starts two host workers: one for its blocking call and one
/// for a nested child call. The test runner gives each case a fresh process,
/// so this world is the first to start the shared pools.
#[cfg(feature = "test-support")]
#[test]
fn a_seeded_world_runs_a_nested_blocking_child_on_its_second_worker() -> io::Result<()> {
    let runtime = crate::testing::seeded_runtime(7)?;
    let (schedule, _events) = crate::testing::Schedule::seeded(7);
    runtime.block_on(schedule.scope(async {
        let outer = crate::run_blocking_io(
            || {
                let child = crate::run_blocking_io(|| 41, None)?;
                crate::testing::host::blocking_on(child)
                    .map_err(|_| io::Error::other("nested child lost its worker"))
            },
            None,
        )?;
        assert_eq!(outer.await??, 41);
        Ok::<_, io::Error>(())
    }))
}
