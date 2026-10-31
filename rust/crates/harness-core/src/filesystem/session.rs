//! Retained preparation belongs to the serving runtime, independently of turns.
use super::PreparationIo;
use crate::{Error, Result, agent::AgentId, sync::SyncMutex};
use acyclic_collections::{FixedIndex, IndexWork, arc_allocation_bytes};
use acyclic_fs::{
    CancellationToken, NativeAllocation, NativeMaterializationWork, WorkCounters,
    materializer::NativeRetainedProducer,
};
use std::sync::Arc;

/// The serving task's exclusive lifetime custody. Consumers cannot retain it.
/// Dropping it cancels retained work and closes the original capacity producer.
pub struct SessionOwner {
    cancellation: CancellationToken,
    _producer: NativeRetainedProducer,
}

impl Drop for SessionOwner {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// One admitted runtime's retained work and lazy executing-agent slots.
/// This is funding, not filesystem authority. Readers still need the executing
/// agent's original volume grants and pinned generation.
pub struct SessionWork {
    agents: SyncMutex<FixedIndex<Arc<AgentSession>, NativeAllocation>>,
    io: PreparationIo,
    _allocation: NativeAllocation,
}

/// A selected agent's retained preparation on the original session ledger.
/// The slot survives turn retirement, but cannot outlive session cancellation.
pub struct AgentSession {
    io: PreparationIo,
    _allocation: NativeAllocation,
}

impl SessionWork {
    /// Moves the runtime's original admission into a fresh cancellation scope.
    /// No remaining allowance is copied or reset. The non-clone owner must be
    /// held by the actual serving task, never by a consumer or an agent turn.
    pub fn admit(original: NativeMaterializationWork) -> Result<(SessionOwner, Arc<Self>)> {
        let (work, cancellation) = original.invocation_view()?;
        let owner = SessionOwner {
            cancellation,
            _producer: work.retained_producer()?,
        };
        let allocation = claim_arc::<Self>(&work)?;
        let io = PreparationIo::from_native_work(&work)?;
        Ok((
            owner,
            Arc::new(Self {
                agents: SyncMutex::new(FixedIndex::default()),
                io,
                _allocation: allocation,
            }),
        ))
    }

    /// Selects the original slot by the complete actor identity. Fixed-width
    /// indexing examines at most 128 branches, independently of other agents.
    /// A repeat shares custody; it does not clone a registry or a turn budget.
    pub fn agent(&self, agent: &AgentId) -> Result<Arc<AgentSession>> {
        let work = self
            .io
            .original_native_work()
            .ok_or_else(|| Error::Invalid("session has no native admission".into()))?;
        let mut accounting = Accounting(work);
        let mut agents = self
            .agents
            .lock()
            .map_err(|_| Error::Invalid("session agent index is poisoned".into()))?;
        let slot = agents.get_or_try_insert_with(agent.to_bytes(), &mut accounting, |work| {
            let allocation = claim_arc::<AgentSession>(work.0)?;
            let io = PreparationIo::from_native_work(work.0)?;
            Ok(Arc::new(AgentSession {
                io,
                _allocation: allocation,
            }))
        })?;
        Ok(Arc::clone(slot))
    }
}

impl AgentSession {
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
