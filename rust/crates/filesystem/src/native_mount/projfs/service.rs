//! The projector service of a shared ProjFS instance: one thread runs the
//! projector passes of every view of the instance, so a view costs no thread
//! of its own. A view is scheduled when its projector has work, runs one pass
//! at a time, and is rescheduled while work remains; a held entry's release
//! schedules its view again. Detaching a view waits for its running pass, after which
//! the service holds nothing of it.

use super::{
    Arc, Condvar, Mutex, NativeMountError, PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT, Readiness,
    Runtime, SendContext, lock_recover,
};
use acyclic_fs_base::ready::{ReadyLinkError, ReadyViews};
use std::sync::Weak;

pub(super) struct ServiceHook {
    service: Weak<Shared>,
    id: u64,
}

impl ServiceHook {
    pub(super) fn schedule(&self) {
        if let Some(service) = self.service.upgrade() {
            service.schedule(self.id);
        }
    }
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    views: ReadyViews<View>,
    running: Option<u64>,
    next: u64,
    stopping: bool,
}

impl State {
    fn check<T>(&mut self, result: Result<T, ReadyLinkError>) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(error) => {
                tracing::error!(%error, "stopping shared projector");
                self.stopping = true;
                None
            }
        }
    }
}

struct View {
    runtime: Arc<Runtime>,
    context: SendContext,
}

impl Shared {
    fn schedule(&self, id: u64) {
        let mut state = lock_recover(&self.state);
        let scheduled = state.views.schedule(id);
        if state.check(scheduled) != Some(false) {
            self.changed.notify_all();
        }
    }

    fn run(&self) {
        let mut state = lock_recover(&self.state);
        loop {
            if state.stopping {
                return;
            }
            let next = state
                .views
                .pop()
                .map(|work| work.map(|(id, view)| (id, Arc::clone(&view.runtime), view.context.0)));
            let Some(next) = state.check(next) else {
                self.changed.notify_all();
                return;
            };
            if let Some((id, runtime, context)) = next {
                state.running = Some(id);
                drop(state);
                let readiness = runtime.projector.service_pass(&runtime, context);
                drop(runtime);
                state = lock_recover(&self.state);
                state.running = None;
                self.changed.notify_all();
                match readiness {
                    Readiness::Ready => {
                        let scheduled = state.views.schedule(id);
                        state.check(scheduled);
                    }
                    Readiness::Idle | Readiness::Stopped => {}
                }
                continue;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

/// One shared instance's projector service. Dropping it stops and joins
/// its thread.
pub(super) struct ProjectorService {
    shared: Arc<Shared>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl ProjectorService {
    pub(super) fn start() -> Result<Self, NativeMountError> {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        });
        let runner = Arc::clone(&shared);
        let worker = std::thread::Builder::new()
            .name("acyclic-projfs-projector".to_owned())
            .spawn(move || runner.run())
            .map_err(NativeMountError::Io)?;
        Ok(Self {
            shared,
            worker: Some(worker),
        })
    }

    /// Makes the service the runner of `runtime`'s projector and schedules
    /// its first pass. Returns the id that detaches it.
    pub(super) fn attach(
        &self,
        runtime: &Arc<Runtime>,
        context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    ) -> Result<u64, NativeMountError> {
        let id = {
            let mut state = lock_recover(&self.shared.state);
            state.next += 1;
            let id = state.next;
            state
                .views
                .insert(
                    id,
                    View {
                        runtime: Arc::clone(runtime),
                        context: SendContext(context),
                    },
                )
                .map_err(|error| NativeMountError::Io(std::io::Error::other(error)))?;
            id
        };
        if runtime
            .projector
            .service
            .set(ServiceHook {
                service: Arc::downgrade(&self.shared),
                id,
            })
            .is_err()
        {
            let mut state = lock_recover(&self.shared.state);
            let removed = state.views.remove(id);
            let refused = state.check(removed);
            self.shared.changed.notify_all();
            drop(refused);
            return Err(NativeMountError::ControlPending);
        }
        self.shared.schedule(id);
        Ok(id)
    }

    /// Removes a view, waiting for its running pass; afterwards the service
    /// holds no reference to it and never runs it again.
    pub(super) fn detach(&self, id: u64) {
        let mut state = lock_recover(&self.shared.state);
        let removed = state.views.remove(id);
        state.check(removed);
        self.shared.changed.notify_all();
        while state.running == Some(id) {
            state = self
                .shared
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

impl Drop for ProjectorService {
    fn drop(&mut self) {
        lock_recover(&self.shared.state).stopping = true;
        self.shared.changed.notify_all();
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            tracing::warn!("ProjFS projector service panicked");
        }
    }
}
