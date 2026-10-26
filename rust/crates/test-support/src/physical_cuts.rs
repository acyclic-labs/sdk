//! Bounded supervisors for independent, cold-process physical crash cuts.
//!
//! Waiting for a child must not occupy a service needed by that child. All
//! callers share one admission bound and join every supervisor before returning
//! an outcome, so a failed cut cannot outlive its fixture owner.
use std::{
    panic::AssertUnwindSafe,
    sync::{Condvar, Mutex},
};

/// Aggregate bound retained from the physical WAL-cut supervisor.
#[must_use]
pub fn workers() -> usize {
    std::thread::available_parallelism()
        .map_or(1, std::num::NonZero::get)
        .saturating_mul(2)
        .clamp(2, 16)
}

static ACTIVE: Mutex<usize> = Mutex::new(0);
static FINISHED: Condvar = Condvar::new();

struct Permit;
impl Permit {
    fn acquire() -> Self {
        let mut active = ACTIVE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *active >= workers() {
            active = FINISHED
                .wait(active)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        *active += 1;
        Self
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut active = ACTIVE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active -= 1;
        drop(active);
        FINISHED.notify_one();
    }
}

/// Execute every independent cut on dedicated supervisors and return sorted IDs.
///
/// `initialize_worker` supplies the host adapter's thread-local guard. It remains
/// alive through all jobs on that worker. No supervisor runs on the calling
/// thread or a host I/O executor.
///
/// # Panics
///
/// Propagates a job panic only after every job and supervisor has completed.
pub fn run<F, G>(jobs: Vec<F>, initialize_worker: impl Fn() -> G + Sync) -> Vec<usize>
where
    F: FnOnce() -> usize + Send,
{
    let worker_count = workers().min(jobs.len());
    let jobs = Mutex::new(jobs.into_iter());
    std::thread::scope(|scope| {
        let supervisors: Vec<_> = (0..worker_count)
            .map(|_| {
                let initialize_worker = &initialize_worker;
                let jobs = &jobs;
                scope.spawn(move || {
                    let _worker = initialize_worker();
                    let mut outcomes = Vec::new();
                    loop {
                        let job = jobs.lock().unwrap().next();
                        let Some(job) = job else { break };
                        outcomes.push(std::panic::catch_unwind(AssertUnwindSafe(|| {
                            let _permit = Permit::acquire();
                            job()
                        })));
                    }
                    outcomes
                })
            })
            .collect();
        let outcomes: Vec<_> = supervisors
            .into_iter()
            .flat_map(|supervisor| supervisor.join().unwrap())
            .collect();
        let mut completed: Vec<_> = outcomes
            .into_iter()
            .map(|outcome| match outcome {
                Ok(point) => point,
                Err(panic) => std::panic::resume_unwind(panic),
            })
            .collect();
        completed.sort_unstable();
        completed
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn independent_cuts_leave_the_callers_thread_and_preserve_every_point() {
        let caller = std::thread::current().id();
        let initialized = AtomicUsize::new(0);
        let jobs: Vec<_> = (0..37)
            .map(|point| {
                move || {
                    assert_ne!(
                        std::thread::current().id(),
                        caller,
                        "a blocking physical cut ran on its caller"
                    );
                    point
                }
            })
            .collect();
        let completed = run(jobs, || {
            initialized.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(completed, (0..37).collect::<Vec<_>>());
        assert_eq!(initialized.load(Ordering::SeqCst), workers().min(37));
    }
}
