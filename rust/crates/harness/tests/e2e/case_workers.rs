//! Drive thread-affine cases within the runner's existing CPU and admission bounds.
use super::{TestResult, progress_guard};
use futures::StreamExt as _;
use std::{collections::VecDeque, future::Future, sync::Mutex};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, watch},
};

pub(super) fn layout(cpu_budget: usize, capacity: usize) -> (usize, usize) {
    assert!(cpu_budget >= 2 && capacity > 0);
    let drivers = (cpu_budget / 2).min(capacity);
    (cpu_budget - drivers, drivers)
}

struct Stop(watch::Sender<bool>);
impl Drop for Stop {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

async fn drive<R>(
    cases: impl futures::Stream<Item = R>,
    send: mpsc::UnboundedSender<R>,
    mut cancel: watch::Receiver<bool>,
) {
    tokio::pin!(cases);
    loop {
        tokio::select! {
            biased;
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() { break; }
            }
            result = cases.next() => match result {
                Some(result) => { if send.send(result).is_err() { break; } }
                None => break,
            }
        }
    }
}

/// Only inputs and results cross threads. Each future is constructed, polled and
/// dropped on its driver; Rc state and the original native-work observer stay local.
pub(super) fn run<T, R, F, Fut>(
    runtime: &Runtime,
    jobs: Vec<T>,
    capacity: usize,
    drivers: usize,
    guard: &mut progress_guard::Guard,
    factory: F,
    mut report: impl FnMut(&R),
) -> TestResult<Vec<R>>
where
    T: Send,
    R: Send,
    F: Fn(T) -> Fut + Sync,
    Fut: Future<Output = R>,
{
    assert!(drivers > 0 && drivers <= capacity);
    let jobs = Mutex::new(VecDeque::from(jobs));
    let (send, receive) = mpsc::unbounded_channel();
    let (stop, cancel) = watch::channel(false);
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for index in 0..drivers {
            // The sum is exactly the original admission capacity, including a
            // remainder. A larger corpus queues without creating more fixtures.
            let slots = capacity / drivers + usize::from(index < capacity % drivers);
            let jobs = &jobs;
            let factory = &factory;
            let send = send.clone();
            let cancel = cancel.clone();
            let handle = runtime.handle().clone();
            workers.push(scope.spawn(move || {
                handle.block_on(async move {
                    let inputs = std::iter::from_fn(|| jobs.lock().unwrap().pop_front());
                    let cases = super::scenario_admission(inputs.map(factory), slots);
                    drive(cases, send, cancel).await;
                });
            }));
        }
        drop(send);
        // Guard::run drops this future before draining accepted native owners.
        // Its drop signals every actual driver, so all case registrations and
        // their futures retire before the accepted completions are drained.
        let fallback = Stop(stop.clone());
        let mut receive = receive;
        let output = runtime.block_on(guard.run_retiring(
            async {
                let _stop = fallback;
                collect_results(&mut receive, &mut report).await
            },
            move || {
                // Quiesce caller polling before the shared Schedule marks CANCEL.
                // Joining a driver drops its futures; accepted completion owners
                // stay in the async/native services and are drained by Guard.
                drop(Stop(stop));
                join_drivers(workers)
            },
        ));
        // Retirement can finish a case whose poll was already in progress.
        // Every sender has retired, so report that final queue before returning
        // a stall or retirement error and discarding the collector's output.
        while let Ok(result) = receive.try_recv() {
            report(&result);
        }
        output.map_err(|error| std::io::Error::other(error).into())
    })
}

// Report completed cases immediately, including when another driver never
// finishes. The supervisor may retire this collector without losing diagnostics.
async fn collect_results<R>(
    receive: &mut mpsc::UnboundedReceiver<R>,
    mut report: impl FnMut(&R),
) -> Vec<R> {
    let mut output = Vec::new();
    while let Some(result) = receive.recv().await {
        report(&result);
        output.push(result);
    }
    output
}

/// A panicking caller cannot leave its siblings polling when shared CANCEL is set.
fn join_drivers<'scope>(
    workers: impl IntoIterator<Item = std::thread::ScopedJoinHandle<'scope, ()>>,
) -> Result<(), String> {
    let mut panicked = false;
    for worker in workers {
        panicked |= worker.join().is_err();
    }
    if panicked {
        Err("scenario driver panicked".to_owned())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn layout_preserves_cpu_and_case_bounds() {
        for cpus in 2..=32 {
            for capacity in 1..=128 {
                let (services, drivers) = layout(cpus, capacity);
                assert_eq!(services + drivers, cpus);
                assert!(services > 0 && drivers > 0 && drivers <= capacity);
                let slots: Vec<_> = (0..drivers)
                    .map(|i| capacity / drivers + usize::from(i < capacity % drivers))
                    .collect();
                assert_eq!(slots.iter().sum::<usize>(), capacity);
                assert!(slots.iter().all(|slots| *slots > 0));
            }
        }
    }

    #[test]
    fn non_send_cases_run_on_distinct_drivers_and_detect_a_serial_driver() -> TestResult {
        for serial in [false, true] {
            let drivers = if serial { 1 } else { layout(6, 6).1 };
            let barrier = Arc::new(Barrier::new(drivers));
            let entered = Arc::new(AtomicUsize::new(0));
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(super::super::BLOCKING_WORKERS)
                .enable_all()
                .build()?;
            let mut guard = progress_guard::Guard::new();
            let output = run(
                &runtime,
                (0..12).collect(),
                6,
                drivers,
                &mut guard,
                |job| {
                    let barrier = barrier.clone();
                    let entered = entered.clone();
                    async move {
                        // Hold Rc across suspension: moving the case to tokio::spawn
                        // would fail to compile and lose the original observer model.
                        let local = std::rc::Rc::new(job);
                        if entered.fetch_add(1, Ordering::SeqCst) < drivers {
                            barrier.wait();
                        }
                        tokio::task::yield_now().await;
                        (*local, std::thread::current().id())
                    }
                },
                |_| {},
            )?;
            let threads: std::collections::HashSet<_> = output.iter().map(|row| row.1).collect();
            let mut jobs: Vec<_> = output.into_iter().map(|row| row.0).collect();
            jobs.sort_unstable();
            assert_eq!(jobs, (0..12).collect::<Vec<_>>());
            assert_eq!(threads.len(), drivers);
            // Both paths finish every job; the former serial driving fails this
            // same physical-thread invariant without relying on elapsed time.
            assert_eq!(threads.len() == 3, !serial);
        }
        Ok(())
    }

    #[test]
    fn driver_cancellation_drops_registered_cases_before_native_cleanup() -> TestResult {
        use futures::FutureExt as _;
        let guard = progress_guard::Guard::new();
        let registry = guard.registry.clone();
        let io = guard.io.clone();
        let dropped = Arc::new(AtomicUsize::new(0));
        struct Retire(Arc<AtomicUsize>);
        impl Drop for Retire {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let case = {
            let dropped = dropped.clone();
            let io = io.clone();
            let registry = registry.clone();
            async move {
                let _retire = Retire(dropped);
                super::super::stall::run(io, registry, "worker-stalled-follow".into(), async {
                    super::super::stall::waiting("status follow", std::future::pending::<()>())
                        .await;
                })
                .await
            }
        };
        let (send, _receive) = mpsc::unbounded_channel();
        let (stop, cancel) = watch::channel(false);
        let cases = futures::stream::once(case);
        let driver = drive(cases, send, cancel);
        tokio::pin!(driver);
        assert!(driver.as_mut().now_or_never().is_none());
        assert_eq!(registry.snapshot().1.len(), 1);
        drop(Stop(stop));
        assert!(
            driver.as_mut().now_or_never().is_some(),
            "driver ignored supervisor cancellation"
        );
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(registry.snapshot().1.is_empty());
        Ok(())
    }
}

#[cfg(test)]
mod retirement_order_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn guard_retires_callers_before_shared_cancellation() {
        let mut guard = progress_guard::Guard::new();
        let io = guard.io.clone();
        let registry = guard.registry.clone();
        let retired = std::cell::Cell::new(false);
        let future = super::super::stall::run(
            io.clone(),
            registry.clone(),
            "retirement-order".into(),
            super::super::stall::waiting("status follow", std::future::pending::<()>()),
        );
        let result = guard
            .run_retiring(future, || {
                assert!(
                    !io.trace().iter().any(|event| event == "CANCEL"),
                    "shared cancellation raced caller retirement"
                );
                assert_eq!(registry.snapshot().1.len(), 1);
                retired.set(true);
                Ok(())
            })
            .await;
        assert!(result.unwrap_err().contains("retirement-order"));
        assert!(retired.get());
        assert!(io.trace().iter().any(|event| event == "CANCEL"));
        assert!(registry.snapshot().1.is_empty());
    }
}

#[cfg(test)]
mod panic_retirement_tests {
    use super::*;

    #[test]
    fn one_panicking_driver_cannot_skip_joining_a_live_sibling() -> TestResult {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        };
        let retired = Arc::new(AtomicBool::new(false));
        std::thread::scope(|scope| -> TestResult {
            let (release, waiting) = mpsc::channel();
            let sibling = retired.clone();
            let workers = vec![
                scope.spawn(|| panic!("driver panic negative control")),
                scope.spawn(move || {
                    waiting.recv().unwrap();
                    sibling.store(true, Ordering::SeqCst);
                }),
            ];
            let (send, receive) = mpsc::channel();
            let retirement = scope.spawn(move || {
                let mut examined = 0;
                let result = join_drivers(workers.into_iter().inspect(|_| {
                    examined += 1;
                    if examined == 2 {
                        send.send("joining sibling").unwrap();
                    }
                }));
                send.send("finished").unwrap();
                result
            });
            // Either the second actual join is reached, or the broken early
            // return finishes first. Release the sibling before any assertion,
            // so both paths terminate without a deadline or stranded thread.
            let first = receive.recv()?;
            release.send(())?;
            let result = retirement.join().expect("retirement helper panicked");
            assert_eq!(
                first, "joining sibling",
                "a panic skipped sibling retirement"
            );
            assert_eq!(result, Err("scenario driver panicked".into()));
            assert!(retired.load(Ordering::SeqCst));
            Ok(())
        })
    }
}

#[cfg(test)]
mod reporting_tests {
    use super::*;
    use futures::FutureExt as _;
    use std::cell::RefCell;

    #[test]
    fn completed_results_are_reported_while_the_next_driver_is_pending() {
        let (send, mut receive) = mpsc::unbounded_channel();
        let reported = RefCell::new(Vec::new());
        send.send("completed failure").unwrap();
        let pending = collect_results(&mut receive, |record| reported.borrow_mut().push(*record));
        futures::pin_mut!(pending);
        assert!(pending.as_mut().now_or_never().is_none());
        assert_eq!(
            *reported.borrow(),
            vec!["completed failure"],
            "completed case report waits for the final driver"
        );
        send.send("another completed result").unwrap();
        assert!(pending.as_mut().now_or_never().is_none());
        assert_eq!(reported.borrow().len(), 2);
        // The sender stays alive, just like a sibling that has not completed.
        // Dropping the receiver cannot erase reports already delivered.
    }
}

#[cfg(test)]
mod shutdown_reporting_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

    #[test]
    fn run_reports_a_completed_failure_queued_during_retirement() -> TestResult {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()?;
        let mut guard = progress_guard::Guard::new();
        let registry = guard.registry.clone();
        let registration = registry.register(std::path::Path::new("stalled-sibling"));
        drop(registration.polling());
        let waiting = registration.waiting("status follow");
        let (release, blocked) = mpsc::channel();
        struct Release {
            send: Option<mpsc::Sender<()>>,
            _waiting: super::super::progress_registry::Waiting,
            _registration: super::super::progress_registry::Registration,
        }
        impl Drop for Release {
            fn drop(&mut self) {
                self.send.take().unwrap().send(()).unwrap();
            }
        }
        let release = Mutex::new(Some(Release {
            send: Some(release),
            _waiting: waiting,
            _registration: registration,
        }));
        let blocked = Mutex::new(blocked);
        let entered = Arc::new(AtomicUsize::new(0));
        // Keep virtual time from advancing until both real owner threads are
        // polling. No wall-clock deadline or sleep determines the race.
        let ready = entered.clone();
        let clock = runtime.spawn(async move {
            while ready.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        });
        let mut reported = Vec::new();
        let result = run(
            &runtime,
            vec![false, true],
            2,
            2,
            &mut guard,
            |complete| {
                let release = &release;
                let blocked = &blocked;
                let entered = &entered;
                async move {
                    if complete {
                        entered.fetch_add(1, Ordering::SeqCst);
                        // This poll finishes only when retirement drops its
                        // pending sibling, after the collector's final poll.
                        blocked.lock().unwrap().recv().unwrap();
                        "completed failure: retained/failure.json"
                    } else {
                        let _release = release.lock().unwrap().take().unwrap();
                        entered.fetch_add(1, Ordering::SeqCst);
                        std::future::pending::<&str>().await
                    }
                }
            },
            |record| reported.push(*record),
        );
        runtime.block_on(clock)?;
        assert!(result.unwrap_err().to_string().contains("stalled-sibling"));
        assert!(registry.snapshot().1.is_empty());
        assert_eq!(
            reported,
            vec!["completed failure: retained/failure.json"],
            "retirement discarded a completed case report"
        );
        Ok(())
    }
}
