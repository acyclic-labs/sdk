//! Retained preparation belongs to the serving runtime, independently of turns.
use super::PreparationIo;
use crate::native_future::{NativeFutureBundle, NativeOrdered};
use crate::{Error, Result, agent::AgentId, sync::SyncMutex};
use acyclic_collections::{FixedIndex, IndexWork, arc_allocation_bytes};
use acyclic_fs::{
    CancellationToken, NativeAllocation, NativeMaterializationWork, WorkCounters,
    materializer::NativeRetainedProducer,
};
use std::{
    collections::VecDeque,
    future::Future,
    sync::Arc,
    task::{Context, Poll, Waker},
};

/// The serving task's exclusive lifetime custody. Consumers cannot retain it.
/// Dropping it cancels retained work and closes the original capacity producer.
pub struct SessionOwner {
    futures: NativeOrdered<NativeFutureBundle<'static, ()>>,
    incoming: Arc<Incoming>,
    cancellation: CancellationToken,
    _producer: NativeRetainedProducer,
}

impl Drop for SessionOwner {
    fn drop(&mut self) {
        self.cancellation.cancel();
        let (pending, waiter) = {
            let mut state = self
                .incoming
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            (std::mem::take(&mut state.pending), state.waiter.take())
        };
        // Future destructors and original producer-drop notifications run
        // outside synchronization, before their queue allocation retires.
        drop(pending);
        drop(waiter);
    }
}

/// One admitted runtime's retained work and lazy executing-agent slots.
/// This is funding, not filesystem authority. Readers still need the executing
/// agent's original volume grants and pinned generation.
pub struct SessionWork {
    #[cfg(feature = "code-mode")]
    code: SyncMutex<Option<Arc<crate::code_mode::CodeRuntime>>>,
    incoming: Arc<Incoming>,
    agents: SyncMutex<FixedIndex<Arc<AgentSession>, NativeAllocation>>,
    io: PreparationIo,
    _allocation: NativeAllocation,
}

/// The executor's current serving owner. A stopped owner can be replaced, but
/// two live runtimes cannot share an executor's retained session.
#[derive(Default)]
pub(super) struct SessionBinding(SyncMutex<Option<Arc<SessionWork>>>);

impl SessionBinding {
    pub(super) fn admit(&self, session: &Arc<SessionWork>) -> Result<()> {
        let previous = {
            let mut current = self
                .0
                .lock()
                .map_err(|_| binding_error("executor session binding is poisoned"))?;
            if session.closed()? {
                return Err(binding_error("serving session owner has terminated"));
            }
            if let Some(original) = current.as_ref() {
                if Arc::ptr_eq(original, session) {
                    return Ok(());
                }
                if !original.closed()? {
                    return Err(binding_error(
                        "executor belongs to another live serving session",
                    ));
                }
            }
            current.replace(Arc::clone(session))
        };
        // Retiring the stopped session may run allocation and producer
        // destructors. None run under the executor's binding lock.
        drop(previous);
        Ok(())
    }

    pub(super) fn get(&self) -> Result<Option<Arc<SessionWork>>> {
        Ok(self
            .0
            .lock()
            .map_err(|_| binding_error("executor session binding is poisoned"))?
            .clone())
    }
}

fn binding_error(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason,
    }
}

/// A selected agent's retained preparation on the original session ledger.
/// The slot survives turn retirement; its work refuses after session cancellation.
pub struct AgentSession {
    incoming: Arc<Incoming>,
    io: PreparationIo,
    _allocation: NativeAllocation,
}

#[derive(Default)]
struct IncomingState {
    pending: VecDeque<NativeFutureBundle<'static, ()>>,
    waiter: Option<Waker>,
    closed: bool,
}

struct Incoming {
    state: SyncMutex<IncomingState>,
    io: PreparationIo,
    _allocation: NativeAllocation,
}

impl SessionOwner {
    /// Drives retained producers beside the original serving loop. Only this
    /// non-clone owner can poll them. Each poll retires at most one completion;
    /// producers and their destructors never run under the handoff lock.
    pub async fn drive(&mut self) -> Result<()> {
        std::future::poll_fn(|context| self.poll(context)).await
    }

    fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<()>> {
        let next_waiter = context.waker().clone();
        let (incoming, remaining, previous) = {
            let mut state = self.incoming.state.lock().map_err(|_| {
                self.incoming
                    .io
                    .invalid("session producer handoff is poisoned")
            })?;
            let previous = state.waiter.replace(next_waiter);
            let incoming = state.pending.pop_front();
            (incoming, !state.pending.is_empty(), previous)
        };
        drop(previous);
        if let Some(incoming) = incoming {
            self.futures.push_back(incoming)?;
        }
        match self.futures.poll_next(context) {
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(Some(result))) => {
                result?;
                // A completed producer is concrete ready work, not a probe.
                context.waker().wake_by_ref();
            }
            Poll::Ready(Ok(None)) | Poll::Pending => {
                if remaining {
                    context.waker().wake_by_ref();
                }
            }
        }
        Poll::Pending
    }
}

impl SessionWork {
    /// Starts a transient preparation frame on the same admitted ledger.
    /// The frame cannot retain the serving producer or reset its allowance.
    #[cfg(feature = "code-mode")]
    pub(crate) fn preparation(&self) -> Result<PreparationIo> {
        self.io.check_cancellation()?;
        let work = self
            .io
            .original_native_work()
            .ok_or_else(|| self.io.invalid("session has no native admission"))?;
        PreparationIo::from_native_work(work)
    }

    fn closed(&self) -> Result<bool> {
        self.incoming
            .state
            .lock()
            .map(|state| state.closed)
            .map_err(|_| self.io.invalid("session producer handoff is poisoned"))
    }

    /// Moves the runtime's original admission into a fresh cancellation scope.
    /// No remaining allowance is copied or reset. The non-clone owner must be
    /// held by the actual serving task, never by a consumer or an agent turn.
    pub fn admit(original: NativeMaterializationWork) -> Result<(SessionOwner, Arc<Self>)> {
        let (work, cancellation) = original.invocation_view()?;
        let producer = work.retained_producer()?;
        let futures = NativeOrdered::completion_order(Some(&work))?;
        let incoming = Arc::new(Incoming {
            state: SyncMutex::new(IncomingState::default()),
            io: PreparationIo::from_native_work(&work)?,
            _allocation: claim_arc::<Incoming>(&work)?,
        });
        let allocation = claim_arc::<Self>(&work)?;
        let io = PreparationIo::from_native_work(&work)?;
        Ok((
            SessionOwner {
                futures,
                incoming: Arc::clone(&incoming),
                cancellation,
                _producer: producer,
            },
            Arc::new(Self {
                #[cfg(feature = "code-mode")]
                code: SyncMutex::new(None),
                incoming,
                agents: SyncMutex::new(FixedIndex::default()),
                io,
                _allocation: allocation,
            }),
        ))
    }

    /// Lazily builds the one interpreter on this session's original ledger.
    /// No agent owns a VM. Retained consumers cannot keep its producer alive.
    #[cfg(feature = "code-mode")]
    pub(crate) fn code_runtime(&self) -> Result<Arc<crate::code_mode::CodeRuntime>> {
        self.io.check_cancellation()?;
        let work = self
            .io
            .original_native_work()
            .ok_or_else(|| self.io.invalid("session has no native admission"))?;
        let mut code = self
            .code
            .lock()
            .map_err(|_| self.io.invalid("session interpreter admission is poisoned"))?;
        if let Some(runtime) = code.as_ref() {
            return Ok(Arc::clone(runtime));
        }
        let runtime = crate::code_mode::CodeRuntime::shared(work)?;
        *code = Some(Arc::clone(&runtime));
        Ok(runtime)
    }

    /// Selects the original slot by the complete actor identity. Each index
    /// traversal examines at most 128 branches, independently of other agents.
    /// A repeat shares custody; it does not clone a registry or a turn budget.
    pub fn agent(&self, agent: &AgentId) -> Result<Arc<AgentSession>> {
        let work = self
            .io
            .original_native_work()
            .ok_or_else(|| self.io.invalid("session has no native admission"))?;
        let mut accounting = Accounting(work);
        let mut agents = self
            .agents
            .lock()
            .map_err(|_| self.io.invalid("session agent index is poisoned"))?;
        let slot = agents.get_or_try_insert_with(agent.to_bytes(), &mut accounting, |work| {
            let allocation = claim_arc::<AgentSession>(work.0)?;
            let io = PreparationIo::from_native_work(work.0)?;
            Ok(Arc::new(AgentSession {
                incoming: Arc::clone(&self.incoming),
                io,
                _allocation: allocation,
            }))
        })?;
        Ok(Arc::clone(slot))
    }
}

impl AgentSession {
    /// Attaches an owned producer to the serving task on this exact session's
    /// ledger. The builder receives the retained slot, never a turn observer.
    /// It runs only when the original owner drives the admitted future. The
    /// producer must publish its result through its original journal/event;
    /// this handoff creates no result store, claim, task or completion protocol.
    pub fn attach<F>(
        self: &Arc<Self>,
        build: impl FnOnce(Arc<Self>) -> F + Send + 'static,
    ) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let work = self
            .preparation()?
            .original_native_work()
            .ok_or_else(|| self.io.invalid("session has no native admission"))?;
        let slot = Arc::clone(self);
        let future = NativeFutureBundle::new(work, || async move {
            build(slot).await;
            Ok(())
        })?;
        let waiter = {
            let mut state = self
                .incoming
                .state
                .lock()
                .map_err(|_| self.io.invalid("session producer handoff is poisoned"))?;
            self.io.check_cancellation()?;
            if state.closed {
                return Err(self.io.invalid("session producer owner has terminated"));
            }
            if state.pending.len() == state.pending.capacity() {
                let capacity = state.pending.len().max(1).checked_mul(2).ok_or_else(|| {
                    self.io
                        .invalid("session producer handoff capacity overflows")
                })?;
                crate::executor::stock_vec_reserve(
                    &mut state.pending,
                    capacity,
                    Some(&self.incoming.io),
                )?;
            }
            state.pending.push_back(future);
            state.waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
        Ok(())
    }

    /// Borrows the original session preparation. A closed runtime refuses even
    /// if an external observer still retains this slot.
    pub fn preparation(&self) -> Result<&PreparationIo> {
        self.io.check_cancellation()?;
        Ok(&self.io)
    }
}

fn claim_arc<T>(work: &NativeMaterializationWork) -> Result<NativeAllocation> {
    let bytes = arc_allocation_bytes::<T>();
    Ok(work.claim(bytes as u64, 1, WorkCounters::UNCHARGED)?)
}

struct Accounting<'a>(&'a NativeMaterializationWork);

impl IndexWork for Accounting<'_> {
    type Error = Error;
    type Allocation = NativeAllocation;

    fn check(&mut self) -> Result<()> {
        self.0.remaining()?;
        Ok(())
    }

    fn charge_items(&mut self, items: u64) -> Result<()> {
        Ok(self.0.charge(WorkCounters {
            items_examined: items,
            ..WorkCounters::UNCHARGED
        })?)
    }

    fn claim(&mut self, bytes: u64) -> Result<NativeAllocation> {
        Ok(self.0.claim(bytes, 1, WorkCounters::UNCHARGED)?)
    }
}

#[cfg(all(test, not(loom)))]
mod tests;
