//! Orphaned head turns: an unsettled head that no caller in this process drives.
//!
//! Durable state never names a driver. A reopened host therefore starts with no
//! drivers, so every unsettled head left by a killed process is orphaned. A new
//! prompt settles an orphan before admission instead of waiting for a driver
//! that can no longer exist. Settlement reconciles every recorded effect through
//! the task's own reconciliation path and never dispatches a model or tool again.
use super::*;
use crate::{distributed::WorkPull, runtime::TaskStateProvider, sync::SyncMutex};
use std::collections::BTreeMap;

/// Bounded wait for recorded effects to report their retained outcome.
const RECONCILE_WAIT_MILLIS: u32 = 30_000;
const RECONCILE_COMMANDS: u32 = 32;

// Every claim is a live `Driving` value: ownership exists only while a value
// that its drop releases holds it.
#[derive(Default)]
struct State {
    active: BTreeMap<OperationId, DriverState>,
}

#[derive(Default)]
struct DriverState {
    claims: usize,
    executing: bool,
}

/// In-process ownership of admitted turns.
pub(super) struct Drivers(SyncMutex<State>);

impl Drivers {
    pub(super) fn new() -> Self {
        Self(SyncMutex::new(State::default()))
    }

    fn state(&self) -> impl std::ops::DerefMut<Target = State> + '_ {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Claims a head. Callers claim under the admission gate, which also
    /// serializes supersession, and carry the claim from admission to driving.
    pub(super) fn drive(&self, operation: OperationId) -> Driving<'_> {
        let mut state = self.state();
        state.active.entry(operation).or_default().claims += 1;
        Driving {
            drivers: self,
            operation,
        }
    }

    pub(super) fn orphaned(&self, operation: OperationId) -> bool {
        #[cfg(test)]
        if NEVER_ORPHANED.with(std::cell::Cell::get) {
            return false;
        }
        !self.state().active.contains_key(&operation)
    }
}

#[cfg(test)]
thread_local! {
    static NEVER_ORPHANED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(in crate::local) static HEAD_ONLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static OVERLAPPING_DRIVERS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Negative control: while entered, no head on this thread is ever orphaned,
/// as before unsettled heads were superseded.
#[cfg(test)]
pub(super) struct NeverOrphaned(());

#[cfg(test)]
impl NeverOrphaned {
    pub(super) fn enter() -> Self {
        NEVER_ORPHANED.with(|flag| flag.set(true));
        Self(())
    }
}

#[cfg(test)]
impl Drop for NeverOrphaned {
    fn drop(&mut self) {
        NEVER_ORPHANED.with(|flag| flag.set(false));
    }
}

/// One claim on a head, released when dropped, including when its future is
/// cancelled at any await between admission and the end of driving.
pub(super) struct Driving<'a> {
    drivers: &'a Drivers,
    operation: OperationId,
}

impl Driving<'_> {
    pub(super) fn operation(&self) -> OperationId {
        self.operation
    }

    /// A live ownership claim can have only one executor. The returned guard
    /// borrows the claim, so its registry entry cannot disappear before release.
    /// Lock order remains admission -> drivers; this leaf lock is never held
    /// across execution, provider callbacks or an await.
    pub(super) fn execute(&self) -> Result<Executing<'_>> {
        let mut state = self.drivers.state();
        let entry = state
            .active
            .get_mut(&self.operation)
            .expect("live driver claim");
        #[cfg(test)]
        let overlapping = OVERLAPPING_DRIVERS.get();
        #[cfg(not(test))]
        let overlapping = false;
        if entry.executing && !overlapping {
            return Err(Error::Conflict(
                "local turn already has an active driver".into(),
            ));
        }
        entry.executing = true;
        Ok(Executing {
            drivers: self.drivers,
            operation: self.operation,
        })
    }
}

pub(super) struct Executing<'a> {
    drivers: &'a Drivers,
    operation: OperationId,
}

impl Drop for Executing<'_> {
    fn drop(&mut self) {
        self.drivers
            .state()
            .active
            .get_mut(&self.operation)
            .expect("execution retains its driver claim")
            .executing = false;
    }
}

impl Drop for Driving<'_> {
    fn drop(&mut self) {
        let mut state = self.drivers.state();
        if let Some(entry) = state.active.get_mut(&self.operation) {
            entry.claims -= 1;
            if entry.claims == 0 {
                state.active.remove(&self.operation);
            }
        }
    }
}

/// Settled outcomes release the reservation. Indeterminate keeps it for a driver.
pub(super) fn settled<T>(outcome: Option<&crate::Outcome<T>>) -> bool {
    outcome.is_some_and(|outcome| !matches!(outcome, crate::Outcome::Indeterminate { .. }))
}

impl LocalHarness {
    /// Settles an orphaned head as Cancelled. The caller holds the admission gate.
    pub(super) async fn supersede(&self, operation: OperationId) -> Result<()> {
        let task = crate::runtime::task_id_for_admission(operation);
        let host = self.runtime.task_host();
        host.cancel(task).await?;
        // Cancellation closes admission for the entire subtree. Collect it before
        // settling: every child must release its own retained reservation, even
        // when cancellation already made the head terminal. A dropped recovery
        // restarts this same walk; settled ancestors never hide unfinished work.
        let mut tasks = vec![task];
        let mut index = 0;
        while index < tasks.len() {
            let mut after = None;
            let mut revision = None;
            loop {
                let page = host.children(tasks[index], revision, after, 64).await?;
                tasks.extend(page.entries.into_iter().map(|child| child.task_id));
                revision = Some(page.revision);
                after = page.next_after;
                if after.is_none() {
                    break;
                }
            }
            index += 1;
        }
        #[cfg(test)]
        if HEAD_ONLY.get() {
            tasks.truncate(1);
        }
        for task in tasks.into_iter().rev() {
            self.settle_orphan(task).await?;
        }
        self.refresh_command_task(task).await
    }

    async fn settle_orphan(&self, task: TaskId) -> Result<()> {
        let host = self.runtime.task_host();
        if !settled(host.terminal_status(task).await?.as_ref()) {
            let lease = match Box::pin(host.recover_work(task, &mut None)).await? {
                WorkPull::Claimed(lease) | WorkPull::Unresolved { lease, .. } => lease,
                WorkPull::Idle => {
                    return Err(Error::Conflict(
                        "orphaned turn lost its reservation before settling".into(),
                    ));
                }
            };
            let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
            // Recorded effects report their retained outcome; unknown ones stay
            // unknown in the journal and are never dispatched again.
            let commands = self.commands()?;
            let _ = self
                .runtime
                .reconcile_task(
                    lease,
                    &commands,
                    RECONCILE_COMMANDS,
                    RECONCILE_WAIT_MILLIS,
                    None,
                )
                .await;
            if !settled(host.terminal_status(task).await?.as_ref()) {
                host.settle_task(task, &fence, crate::Outcome::Cancelled, &mut None)
                    .await?;
            }
        }
        Ok(())
    }
}

// A new prompt supersedes only a head no driver holds. The real gate and the
// real driver registry decide; the shared cell stands in for the outcome.
#[cfg(all(test, acyclic_harness_loom))]
mod model {
    use super::*;
    use crate::sync::owned::{Arc as StateArc, Mutex};
    use loom::{sync::Mutex as DataMutex, thread};
    use std::task::{Context, Poll, Waker};

    fn complete<F: Future>(future: F) -> F::Output {
        match std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("model operation did not finish"),
        }
    }

    fn race(claim_outside_gate: bool) {
        let gate = StateArc::new(Mutex::new(()));
        let drivers = StateArc::new(Drivers::new());
        let operation = OperationId::from_bytes([1; 16]);
        // (settled, settlements)
        let outcome = StateArc::new(DataMutex::new((false, 0)));
        let resume = {
            let (gate, drivers, outcome) = (gate.clone(), drivers.clone(), outcome.clone());
            thread::spawn(move || {
                let _driving = if claim_outside_gate {
                    drivers.drive(operation)
                } else {
                    complete(super::super::admission::serialized(&gate, async || {
                        drivers.drive(operation)
                    }))
                };
                let unsettled = !outcome.lock().unwrap().0;
                thread::yield_now();
                if unsettled {
                    let mut outcome = outcome.lock().unwrap();
                    outcome.0 = true;
                    outcome.1 += 1;
                }
            })
        };
        complete(super::super::admission::serialized(&gate, async || {
            let unsettled = !outcome.lock().unwrap().0;
            if unsettled && drivers.orphaned(operation) {
                thread::yield_now();
                // `supersede` rereads: a driver may settle and leave between
                // the head read and the orphan check.
                let mut outcome = outcome.lock().unwrap();
                if !outcome.0 {
                    outcome.0 = true;
                    outcome.1 += 1;
                }
            }
        }));
        resume.join().unwrap();
        let outcome = outcome.lock().unwrap();
        assert!(outcome.0, "orphan was never settled");
        assert_eq!(outcome.1, 1, "driven turn was superseded");
    }

    // A submitter's claim, taken where admission commits, is carried across
    // the awaits before driving; the future is cancelled at one of them.
    fn cancelled_handoff(claim_outlives_cancellation: bool) {
        let gate = StateArc::new(Mutex::new(()));
        let drivers = StateArc::new(Drivers::new());
        let operation = OperationId::from_bytes([1; 16]);
        let submitter = {
            let (gate, drivers) = (gate.clone(), drivers.clone());
            thread::spawn(move || {
                let claim = complete(super::super::admission::serialized(&gate, async || {
                    drivers.drive(operation)
                }));
                thread::yield_now();
                // The defect: ownership that is not released by the drop
                // that cancellation performs.
                if claim_outlives_cancellation {
                    std::mem::forget(claim);
                }
            })
        };
        submitter.join().unwrap();
        let after = complete(super::super::admission::serialized(&gate, async || {
            drivers.orphaned(operation)
        }));
        assert!(after, "cancelled submission kept its head");
    }

    // A retried admission of the unsettled head hands its claim to driving
    // while another prompt checks the head for orphans.
    fn retried_handoff(claim_at_admission: bool) {
        let gate = StateArc::new(Mutex::new(()));
        let drivers = StateArc::new(Drivers::new());
        let operation = OperationId::from_bytes([1; 16]);
        // (accepted, superseded)
        let head = StateArc::new(DataMutex::new((false, false)));
        let retry = {
            let (gate, drivers, head) = (gate.clone(), drivers.clone(), head.clone());
            thread::spawn(move || {
                let claim = complete(super::super::admission::serialized(&gate, async || {
                    head.lock().unwrap().0 = true;
                    claim_at_admission.then(|| drivers.drive(operation))
                }));
                thread::yield_now();
                let _driving = match claim {
                    Some(claim) => claim,
                    None => complete(super::super::admission::serialized(&gate, async || {
                        drivers.drive(operation)
                    })),
                };
                thread::yield_now();
            })
        };
        complete(super::super::admission::serialized(&gate, async || {
            if drivers.orphaned(operation) {
                let mut head = head.lock().unwrap();
                head.1 = head.0;
            }
        }));
        retry.join().unwrap();
        assert!(!head.lock().unwrap().1, "accepted retry was superseded");
    }

    fn fails_with(model: impl Fn() + Sync + Send + 'static, expected: &str) {
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loom::model(model)))
            .expect_err("negative control did not fail");
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(message.contains(expected), "{message}");
    }

    fn exclusive_drivers(overlapping: bool) {
        let gate = StateArc::new(Mutex::new(()));
        let drivers = StateArc::new(Drivers::new());
        let active = StateArc::new(DataMutex::new(0));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let (gate, drivers, active) = (gate.clone(), drivers.clone(), active.clone());
            threads.push(thread::spawn(move || {
                let claim = complete(super::super::admission::serialized(&gate, async || {
                    drivers.drive(OperationId::from_bytes([1; 16]))
                }));
                OVERLAPPING_DRIVERS.set(overlapping);
                if let Ok(_executing) = claim.execute() {
                    {
                        let mut active = active.lock().unwrap();
                        *active += 1;
                        assert_eq!(*active, 1, "two drivers entered recovery");
                    }
                    thread::yield_now();
                    *active.lock().unwrap() -= 1;
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(*active.lock().unwrap(), 0);
    }

    #[test]
    fn only_one_driver_can_execute_or_reconcile_a_turn() {
        loom::model(|| exclusive_drivers(false));
        fails_with(|| exclusive_drivers(true), "two drivers entered recovery");
    }

    #[test]
    fn a_cancelled_submission_leaves_its_head_to_the_next_prompt() {
        loom::model(|| cancelled_handoff(false));
        fails_with(
            || cancelled_handoff(true),
            "cancelled submission kept its head",
        );
    }

    #[test]
    fn a_retried_admission_keeps_its_head_until_driven() {
        loom::model(|| retried_handoff(true));
        fails_with(|| retried_handoff(false), "accepted retry was superseded");
    }

    #[test]
    fn new_prompt_never_supersedes_a_driven_turn() {
        loom::model(|| race(false));
        fails_with(|| race(true), "driven turn was superseded");
    }
}
