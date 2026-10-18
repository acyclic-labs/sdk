use super::{BLOCKING_WORKERS, TestResult, scenario_builder};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

#[test]
fn blocking_pool_queues_work_instead_of_creating_another_thread() -> TestResult {
    for broken in [false, true] {
        let created = Arc::new(AtomicUsize::new(0));
        let names = created.clone();
        const ASYNC_WORKERS: usize = 2;
        let mut builder = scenario_builder();
        builder.worker_threads(ASYNC_WORKERS);
        if broken {
            builder.max_blocking_threads(BLOCKING_WORKERS + 1);
        }
        // Tokio calls this synchronously when requesting a thread. No sampling
        // or scheduling delay can hide creation of a worker beyond the bound.
        let runtime = builder
            .thread_name_fn(move || {
                names.fetch_add(1, Ordering::SeqCst);
                "e2e-budget".into()
            })
            .build()?;
        let mut jobs = Vec::new();
        let mut releases = Vec::new();
        for _ in 0..BLOCKING_WORKERS {
            let (entered, waiting) = mpsc::sync_channel(0);
            let (release, gate) = mpsc::channel();
            jobs.push(runtime.spawn_blocking(move || -> TestResult {
                entered.send(())?;
                gate.recv()?;
                Ok(())
            }));
            releases.push(release);
            waiting.recv()?;
        }
        let queued = runtime.spawn_blocking(|| 42);
        let threads = created.load(Ordering::SeqCst);
        // Release before asserting so a failure cannot strand the pool on drop.
        for release in releases {
            release.send(())?;
        }
        let answer = runtime.block_on(async {
            for job in jobs {
                job.await??;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(queued.await?)
        })?;
        assert_eq!(answer, 42, "queued work was discarded");
        let bound = ASYNC_WORKERS + BLOCKING_WORKERS;
        assert_eq!(threads, bound + usize::from(broken));
        assert_eq!(threads <= bound, !broken, "blocking-pool control escaped");
    }
    Ok(())
}

#[test]
fn async_pool_matches_the_bounded_workload_and_detects_one_extra_worker() -> TestResult {
    let available = std::thread::available_parallelism().map_or(1, usize::from);
    let expected = available.clamp(2, super::MAX_ASYNC_WORKERS);
    for broken in [false, true] {
        let created = Arc::new(AtomicUsize::new(0));
        let names = created.clone();
        let mut builder = scenario_builder();
        if broken {
            builder.worker_threads(expected + 1);
        }
        let runtime = builder
            .thread_name_fn(move || {
                names.fetch_add(1, Ordering::SeqCst);
                "e2e-async-budget".into()
            })
            .build()?;
        let actual = created.load(Ordering::SeqCst);
        drop(runtime);
        assert_eq!(actual, expected + usize::from(broken));
        assert_eq!(actual == expected, !broken);
    }
    Ok(())
}

std::thread_local! {
    static ADMISSION_CAP: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

pub(super) fn admission_capacity(count: usize) -> usize {
    ADMISSION_CAP.get().unwrap_or(count)
}

fn probe_admission(count: usize, capacity: usize, broken: bool) -> (usize, Vec<usize>) {
    use futures::{FutureExt as _, Stream as _, StreamExt as _, channel::oneshot};
    use std::{cell::Cell, pin::pin, rc::Rc, task::Context};
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            ADMISSION_CAP.set(self.0);
        }
    }
    // The fault restores the actual former buffer_unordered(8) capacity at the
    // production choke point. No case is released until admission is observed.
    let _restore = Restore(ADMISSION_CAP.replace(broken.then_some(8)));
    let entered = Rc::new(Cell::new(0));
    let mut releases = Vec::new();
    let cases = (0..count)
        .map(|index| {
            let (release, gate) = oneshot::channel();
            releases.push(release);
            let entered = Rc::clone(&entered);
            async move {
                entered.set(entered.get() + 1);
                gate.await.expect("admission gate was discarded");
                index
            }
        })
        .collect::<Vec<_>>();
    let mut admitted = pin!(super::scenario_admission(cases.into_iter(), capacity));
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    assert!(admitted.as_mut().poll_next(&mut context).is_pending());
    let started = entered.get();
    for release in releases {
        release.send(()).expect("case was discarded");
    }
    let mut results = admitted
        .collect::<Vec<_>>()
        .now_or_never()
        .expect("all case gates were already released");
    results.sort_unstable();
    (started, results)
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn all_blocked_cases_are_admitted_before_any_finishes(count in 9usize..32) {
        for broken in [false, true] {
            let (started, results) = probe_admission(count, count, broken);
            proptest::prop_assert_eq!(results, (0..count).collect::<Vec<_>>());
            proptest::prop_assert_eq!(started, if broken { 8 } else { count });
            // Both paths finish every case; only the admission assertion rejects
            // the old cap, independently of worker creation and elapsed time.
            let accepted = std::panic::catch_unwind(|| assert_eq!(started, count)).is_ok();
            proptest::prop_assert_eq!(accepted, !broken);
        }
    }

    #[test]
    fn admission_fills_the_requested_capacity_without_exceeding_it(count in 9usize..32, capacity in 1usize..8) {
        for broken in [false, true] {
            let (started, results) = probe_admission(count, capacity, broken);
            proptest::prop_assert_eq!(results, (0..count).collect::<Vec<_>>());
            proptest::prop_assert_eq!(started, if broken { 8 } else { capacity });
            proptest::prop_assert_eq!(started == capacity, !broken);
        }
    }
}
