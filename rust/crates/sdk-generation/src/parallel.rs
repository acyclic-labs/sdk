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

pub(crate) fn map_with_workers<T, R, F>(inputs: Vec<T>, workers: usize, operation: F) -> Vec<R>
where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + Send + Sync + 'static,
{
    assert!(
        (1..=MAX_WORKERS).contains(&workers),
        "invalid owner worker budget"
    );
    let owners = inputs.len();
    let workers = workers.min(owners);
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
    // The queue assigns every owner its original ordinal. Assemble directly;
    // comparison-sorting completed owners adds no ordering guarantee.
    let mut completed = std::iter::repeat_with(|| None)
        .take(owners)
        .collect::<Vec<_>>();
    for (index, result) in join_all(jobs).into_iter().flatten() {
        completed[index] = Some(result);
    }
    completed
        .into_iter()
        .map(|result| {
            result
                .expect("owner queue drained without its completion")
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        })
        .collect()
}

fn join_all<R>(jobs: Vec<thread::JoinHandle<R>>) -> Vec<R> {
    let joined = jobs.into_iter().map(|job| job.join()).collect::<Vec<_>>();
    joined
        .into_iter()
        .map(|result| result.unwrap_or_else(|panic| std::panic::resume_unwind(panic)))
        .collect()
}
