#[cfg(loom)]
pub(crate) use loom::sync::Arc;
#[cfg(loom)]
use loom::{sync::Mutex, thread};
#[cfg(not(loom))]
pub(crate) use std::sync::Arc;
#[cfg(not(loom))]
use std::{sync::Mutex, thread};

const DEFAULT_WORKERS: usize = 4;
const MAX_WORKERS: usize = 32;

// Callers provide disjoint outputs. Every admitted worker drains before errors
// or panics reach publication. A worker also finishes queued owners after an
// operation panic, retaining the original all-owner completion barrier.
pub fn map<T, R, F>(inputs: Vec<T>, operation: F) -> Vec<R>
where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + Send + Sync + 'static,
{
    let workers = match std::env::var("ACYCLIC_SDK_JOBS") {
        Ok(value) => value.parse().ok().filter(|value| (1..=MAX_WORKERS).contains(value))
            .unwrap_or_else(|| panic!("STEP_FAILED owner-worker-budget: ACYCLIC_SDK_JOBS must be 1..=32; recovery: env -u ACYCLIC_SDK_JOBS cargo run --locked -p sdk-generation -- --help")),
        Err(std::env::VarError::NotPresent) => DEFAULT_WORKERS,
        Err(_) => panic!("STEP_FAILED owner-worker-budget: non-Unicode ACYCLIC_SDK_JOBS; recovery: env -u ACYCLIC_SDK_JOBS cargo run --locked -p sdk-generation -- --help"),
    };
    map_with_workers(inputs, workers, operation)
}

fn map_with_workers<T, R, F>(inputs: Vec<T>, workers: usize, operation: F) -> Vec<R>
where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + Send + Sync + 'static,
{
    assert!(
        (1..=MAX_WORKERS).contains(&workers),
        "invalid owner worker budget"
    );
    let workers = workers.min(inputs.len());
    let inputs = Arc::new(Mutex::new(inputs.into_iter().enumerate()));
    let operation = Arc::new(operation);
    let mut jobs = Vec::with_capacity(workers);
    for _ in 0..workers {
        let inputs = Arc::clone(&inputs);
        let operation = Arc::clone(&operation);
        match thread::Builder::new().spawn(move || {
            let mut completed = Vec::new();
            loop {
                let input = inputs.lock().expect("owner queue poisoned").next();
                let Some((index, input)) = input else { break };
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(input)));
                completed.push((index, result));
            }
            completed
        }) {
            Ok(job) => jobs.push(job),
            Err(error) => {
                drop(join_all(jobs));
                panic!("cannot start owner compiler: {error}");
            }
        }
    }
    let mut completed = join_all(jobs).into_iter().flatten().collect::<Vec<_>>();
    completed.sort_unstable_by_key(|(index, _)| *index);
    completed
        .into_iter()
        .map(|(_, result)| result.unwrap_or_else(|panic| std::panic::resume_unwind(panic)))
        .collect()
}

fn join_all<R>(jobs: Vec<thread::JoinHandle<R>>) -> Vec<R> {
    let joined = jobs.into_iter().map(|job| job.join()).collect::<Vec<_>>();
    joined
        .into_iter()
        .map(|result| result.unwrap_or_else(|panic| std::panic::resume_unwind(panic)))
        .collect()
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn bounded_workers_preserve_input_order_and_visit_every_owner() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let workers = Arc::new(Mutex::new(std::collections::HashSet::new()));
        let worker_ids = Arc::clone(&workers);
        let (running, observed) = (Arc::clone(&active), Arc::clone(&peak));
        let result = map_with_workers((0..100).collect(), 2, move |index| {
            worker_ids.lock().unwrap().insert(thread::current().id());
            let count = running.fetch_add(1, Ordering::SeqCst) + 1;
            observed.fetch_max(count, Ordering::SeqCst);
            thread::yield_now();
            running.fetch_sub(1, Ordering::SeqCst);
            index * 2
        });
        assert_eq!(result, (0..100).map(|index| index * 2).collect::<Vec<_>>());
        assert!(
            workers.lock().unwrap().len() <= 2,
            "owner thread budget exceeded"
        );
        assert!(peak.load(Ordering::SeqCst) <= 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn panic_drains_queued_owners_before_propagating() {
        let count = Arc::new(AtomicUsize::new(0));
        let completed = Arc::clone(&count);
        assert!(std::panic::catch_unwind(move || {
            map_with_workers((0..20).collect(), 1, move |index| {
                completed.fetch_add(1, Ordering::SeqCst);
                assert_ne!(index, 0, "first owner failed");
            })
        })
        .is_err());
        assert_eq!(count.load(Ordering::SeqCst), 20);
    }
}
