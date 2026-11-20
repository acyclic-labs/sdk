//! The Loom executor and mutant registry production snapshot jobs use under
//! `cfg(loom)`. Model drivers live in the test targets that run them.
use std::{cell::RefCell, future::Future, pin::Pin};

/// A task the model executor holds instead of spawning it.
pub type Worker = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Targeted production mutants, selected per model execution.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mutant {
    #[default]
    None,
    /// An abandoned job keeps its queued or granted lock reservation.
    LeakReservation,
    /// Dropping the consumer does not wake a queued job.
    NoWake,
    /// Completion leaves the job registered for a cancellation wake.
    ArmedAfterCompletion,
}

loom::thread_local! {
    static MUTANT: std::cell::Cell<Mutant> = std::cell::Cell::new(Mutant::None);
}

pub fn set_mutant(mutant: Mutant) {
    MUTANT.with(|cell| cell.set(mutant));
}

pub fn mutant() -> Mutant {
    MUTANT.with(std::cell::Cell::get)
}

loom::thread_local! {
    // The model owns executor task storage, just as Tokio does. Dropping these
    // futures exercises the production Job's cancellation and ownership return.
    static EXECUTOR: RefCell<Option<(bool, Vec<Worker>)>> = RefCell::new(None);
}

pub fn capture(discard_on_drop: bool) {
    EXECUTOR.with(|executor| {
        assert!(
            executor
                .borrow_mut()
                .replace((discard_on_drop, Vec::new()))
                .is_none()
        );
    });
}

pub fn discard_on_drop() -> bool {
    EXECUTOR.with(|executor| executor.borrow().as_ref().is_some_and(|entry| entry.0))
}

/// Returns the executor tasks dispatched by the observed consumer poll.
/// Wake assertions are meaningful only after these independently owned jobs
/// have had a chance to finish, just as on a running production executor.
pub fn take_pending() -> Vec<Worker> {
    EXECUTOR.with(|executor| {
        executor
            .borrow_mut()
            .take()
            .map(|(_, workers)| workers)
            .unwrap_or_default()
    })
}

pub fn spawn(worker: impl Future<Output = ()> + Send + 'static) {
    let worker: Worker = Box::pin(worker);
    let worker = EXECUTOR.with(|executor| {
        let mut executor = executor.borrow_mut();
        if let Some((_, workers)) = executor.as_mut() {
            workers.push(worker);
            None
        } else {
            Some(worker)
        }
    });
    if let Some(worker) = worker {
        loom::thread::spawn(move || super::park(worker));
    }
}
