//! Targeted journal changes wake the retained native worker, including after
//! the future awaiting that worker has been dropped.
use super::*;
use acyclic_native_runtime::ProcessWake;
use std::sync::{
    Condvar, Mutex, OnceLock, Weak,
    atomic::{AtomicBool, Ordering},
};
#[cfg(test)]
mod tests;

struct State {
    dirty: AtomicBool,
    signals: crate::durable_host::OwnerSignals,
    gate: Mutex<Gate>,
    checked: Condvar,
    wake: ProcessWake,
    driver: OnceLock<tokio::task::AbortHandle>,
}

#[derive(Default)]
struct Gate {
    failure: Option<String>,
    checking: bool,
}

impl Drop for State {
    fn drop(&mut self) {
        if let Some(driver) = self.driver.get() {
            driver.abort();
        }
    }
}

/// One running command's authenticated ownership-change control.
#[doc(hidden)]
#[derive(Clone)]
pub struct OwnerControl(Arc<State>);

impl OwnerControl {
    /// Subscribes before initial physical dispatch verification. The worker
    /// retains this control through reaping, even if its awaiting task exits.
    pub async fn open<P: StreamProvider + 'static>(
        owner: Arc<TaskJournalOwner<P>>,
    ) -> Result<Self> {
        let mut changes = owner.owner_changes().await?;
        let state = Arc::new(State {
            dirty: AtomicBool::new(true),
            signals: changes.signals(),
            gate: Mutex::new(Gate::default()),
            checked: Condvar::new(),
            wake: ProcessWake::new().map_err(io_error)?,
            driver: OnceLock::new(),
        });
        let active = Arc::downgrade(&state);
        let driver = tokio::spawn(async move {
            loop {
                let result = match changes.changed().await {
                    Ok(()) => owner.verify(false, &mut None).await,
                    Err(cause) => Err(cause),
                };
                let failed = result.is_err();
                if !publish(&active, result) || failed {
                    return;
                }
            }
        });
        let _ = state.driver.set(driver.abort_handle());
        Ok(Self(state))
    }

    /// Signal passed to the kernel readiness wait owned by the native worker.
    #[must_use]
    pub fn wake(&self) -> ProcessWake {
        self.0.wake.clone()
    }

    /// Reauthenticates once at dispatch and after each relevant change. The
    /// flag is cleared before verification so a concurrent change stays dirty.
    pub fn check(&self, verify: impl FnOnce() -> std::io::Result<()>) -> std::io::Result<()> {
        // A resumed worker or a just-cancelled invocation must not use a
        // cached authorization before its asynchronous driver gets a turn.
        if self.0.signals.needs_verification() {
            self.0.dirty.store(true, Ordering::Release);
        }
        let mut gate = self
            .0
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(cause) = &gate.failure {
                return Err(control_error(cause.clone()));
            }
            if !gate.checking {
                break;
            }
            gate = self
                .0
                .checked
                .wait(gate)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if !self.0.dirty.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        gate.checking = true;
        // No runtime task waits on this gate while the native worker drives
        // its asynchronous verification. Concurrent hooks wait on the worker.
        drop(gate);
        let verified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(verify));
        let mut gate = self
            .0
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        gate.checking = false;
        match &verified {
            Ok(Err(cause)) => {
                gate.failure.get_or_insert_with(|| cause.to_string());
            }
            Err(_) => {
                gate.failure
                    .get_or_insert_with(|| "owner verification panicked".into());
            }
            Ok(Ok(())) => {}
        }
        let failure = gate.failure.clone();
        self.0.checked.notify_all();
        drop(gate);
        if failure.is_some() {
            let _ = self.0.wake.stop();
        }
        match verified {
            Ok(Err(cause)) => Err(cause),
            Ok(Ok(())) => failure.map_or(Ok(()), |cause| Err(control_error(cause))),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

fn publish(active: &Weak<State>, result: Result<()>) -> bool {
    let Some(active) = active.upgrade() else {
        return false;
    };
    let failed = result.is_err();
    if let Err(cause) = result {
        active
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failure
            .get_or_insert_with(|| cause.to_string());
    }
    active.dirty.store(true, Ordering::Release);
    active.checked.notify_all();
    // The signal stays owned by this same state; a full coalescing queue is
    // already readable. Any unexpected OS failure is retained as control loss.
    if let Err(cause) = if failed {
        active.wake.stop()
    } else {
        active.wake.notify()
    } {
        active
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failure
            .get_or_insert_with(|| cause.to_string());
        active.checked.notify_all();
        return false;
    }
    true
}
