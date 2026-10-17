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

#[derive(Default)]
struct State {
    active: BTreeMap<OperationId, usize>,
    // A freshly published head belongs to its submitter until it drives it.
    submitted: Option<OperationId>,
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

    pub(super) fn submitted(&self, operation: OperationId) {
        self.state().submitted = Some(operation);
    }

    /// Callers claim under the admission gate, which also serializes supersession.
    pub(super) fn drive(&self, operation: OperationId) -> Driving<'_> {
        let mut state = self.state();
        *state.active.entry(operation).or_default() += 1;
        if state.submitted == Some(operation) {
            state.submitted = None;
        }
        Driving {
            drivers: self,
            operation,
        }
    }

    pub(super) fn orphaned(&self, operation: OperationId) -> bool {
        let state = self.state();
        !state.active.contains_key(&operation) && state.submitted != Some(operation)
    }
}

/// Releases one driver claim, including when its future is dropped mid-turn.
pub(super) struct Driving<'a> {
    drivers: &'a Drivers,
    operation: OperationId,
}

impl Drop for Driving<'_> {
    fn drop(&mut self) {
        let mut state = self.drivers.state();
        if let Some(count) = state.active.get_mut(&self.operation) {
            *count -= 1;
            if *count == 0 {
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
        match host.outcome(task).await {
            Ok(outcome) if settled(outcome.as_ref()) => return Ok(()),
            Ok(_) => {}
            // An unadmitted request has nothing to settle.
            Err(Error::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error),
        }
        host.cancel(task).await?;
        if !settled(host.outcome(task).await?.as_ref()) {
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
            if !settled(host.outcome(task).await?.as_ref()) {
                host.settle_task(task, &fence, crate::Outcome::Cancelled, &mut None)
                    .await?;
            }
        }
        self.refresh_command_task(task).await
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

    #[test]
    fn new_prompt_never_supersedes_a_driven_turn() {
        loom::model(|| race(false));
        let failure = std::panic::catch_unwind(|| loom::model(|| race(true)))
            .expect_err("ungated claim negative control did not fail");
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(message.contains("driven turn was superseded"), "{message}");
    }
}
