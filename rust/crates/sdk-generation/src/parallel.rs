#[cfg(acyclic_sdk_docs_loom)]
pub(crate) use loom::sync::Arc;
#[cfg(acyclic_sdk_docs_loom)]
use loom::thread;
#[cfg(not(acyclic_sdk_docs_loom))]
pub(crate) use std::sync::Arc;
#[cfg(not(acyclic_sdk_docs_loom))]
use std::thread;

// Callers provide disjoint outputs. No lock is held while spawning or joining;
// worker initialization uses the existing leaf slots. Join every worker before
// exposing any result, including errors and panics, to the publication path.
pub fn map<T, R, F>(inputs: Vec<T>, operation: F) -> Vec<R>
where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> R + Send + Sync + 'static,
{
    let operation = Arc::new(operation);
    let mut jobs = Vec::new();
    for input in inputs {
        let operation = Arc::clone(&operation);
        // Keep unwinds as values until every worker has joined. This gives the
        // modeled thread path the same join-error semantics as native threads.
        match thread::Builder::new().spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(input)))
        }) {
            Ok(job) => jobs.push(job),
            Err(error) => {
                drop(join_all(jobs));
                panic!("cannot start owner compiler: {error}");
            }
        }
    }
    join_all(jobs)
}

fn join_all<R>(jobs: Vec<thread::JoinHandle<std::thread::Result<R>>>) -> Vec<R> {
    let joined = jobs
        .into_iter()
        .map(|job| job.join().and_then(|result| result))
        .collect::<Vec<_>>();
    joined
        .into_iter()
        .map(|result| result.unwrap_or_else(|panic| std::panic::resume_unwind(panic)))
        .collect()
}
