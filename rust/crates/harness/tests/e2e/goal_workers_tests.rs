use super::*;

#[test]
fn cancelling_a_goal_driver_cancels_its_original_worker() -> TestResult {
    crate::scenario_runtime()?.block_on(async {
        struct Retired(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Retired {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let (retired, retirement) = tokio::sync::oneshot::channel();
        let continued = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = continued.clone();
        let mut work = Box::pin(crate::goal_workers::scheduled(async move {
            let _retired = Retired(Some(retired));
            started.send(()).unwrap();
            released.await.unwrap();
            continued.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
        tokio::select! {
            result = &mut work => panic!("goal worker completed before release: {result:?}"),
            result = ready => result?,
        }
        drop(work);
        // A detached-worker negative control finishes here instead of hanging.
        let _ = release.send(());
        retirement.await?;
        assert!(!observed.load(std::sync::atomic::Ordering::SeqCst));
        Ok(())
    })
}

#[test]
fn goal_work_reaches_runtime_workers_and_inline_control_fails() -> TestResult {
    let runtime = crate::scenario_runtime()?;
    for inline in [false, true] {
        let (caller, worker) = crate::goal_workers::with_inline(inline, || {
            runtime.block_on(async {
                let caller = std::thread::current().id();
                let worker =
                    crate::goal_workers::scheduled(async { std::thread::current().id() }).await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((caller, worker))
            })
        })?;
        let accepted = std::panic::catch_unwind(|| assert_ne!(caller, worker)).is_ok();
        assert_eq!(
            accepted, !inline,
            "inline execution escaped the worker oracle"
        );
    }
    Ok(())
}

#[test]
fn goal_worker_preserves_the_original_native_schedule() -> TestResult {
    crate::scenario_runtime()?.block_on(async {
        let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::new();
        let work = schedule.scope(crate::goal_workers::scheduled(async { 42 }));
        tokio::pin!(work);
        let mut phases = Vec::new();
        let answer = loop {
            tokio::select! {
                result = &mut work => break result?,
                event = boundaries.recv() => {
                    let event = event.ok_or("goal schedule ended")?;
                    phases.push(event.name);
                    event.release();
                }
            }
        };
        assert_eq!(answer, 42);
        assert_eq!(phases, ["task:submitted", "task:completed"]);
        schedule.drained().await;
        assert_eq!(schedule.pending_native_work(), 0);
        Ok(())
    })
}
