//! Bounded snapshot I/O polled beside demand and followed records by one owner.
use super::*;
use futures::{
    StreamExt as _,
    future::LocalBoxFuture,
    stream::{AbortHandle, Abortable, FuturesUnordered},
};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

type Completed = (StreamPath, u64, crate::task_volume::Result<HeadSnapshot>);
struct PendingSnapshot<'a> {
    agent: StreamPath,
    generation: u64,
    future: Rc<RefCell<Option<LocalBoxFuture<'a, crate::task_volume::Result<HeadSnapshot>>>>>,
}
impl Future for PendingSnapshot<'_> {
    type Output = Completed;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Completed> {
        self.future
            .borrow_mut()
            .as_mut()
            .expect("owned until aborted")
            .as_mut()
            .poll(cx)
            .map(|result| (self.agent.clone(), self.generation, result))
    }
}
struct Owner<'a> {
    generation: u64,
    future: Rc<RefCell<Option<LocalBoxFuture<'a, crate::task_volume::Result<HeadSnapshot>>>>>,
    abort: AbortHandle,
}
impl Drop for Owner<'_> {
    fn drop(&mut self) {
        // Event-loop ownership: no mutex, provider poll or await during removal.
        self.future.borrow_mut().take();
        self.abort.abort();
    }
}
pub(super) struct Snapshots<'a> {
    generation: u64,
    maximum: usize,
    active: BTreeMap<StreamPath, Owner<'a>>,
    futures: FuturesUnordered<Abortable<PendingSnapshot<'a>>>,
}
impl<'a> Snapshots<'a> {
    pub fn new(maximum: usize) -> Self {
        Self {
            generation: 0,
            maximum,
            active: BTreeMap::new(),
            futures: FuturesUnordered::new(),
        }
    }
    pub fn layout(&mut self, demand: &ClientInterests) -> Result<()> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("snapshot generation overflow".into()))?;
        self.active.retain(|agent, _| {
            demand
                .tier(agent)
                .is_some_and(|tier| tier != crate::client::InterestTier::Overview)
        });
        Ok(())
    }
    pub fn retire(&mut self, agent: &StreamPath) -> Result<()> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("snapshot generation overflow".into()))?;
        self.active.remove(agent);
        Ok(())
    }
    pub fn start<F>(&mut self, agent: StreamPath, future: F) -> Result<()>
    where
        F: Future<Output = crate::task_volume::Result<HeadSnapshot>> + 'a,
    {
        if self.active.contains_key(&agent) || self.active.len() >= self.maximum {
            return Err(Error::Invalid(
                "snapshot request exceeds selected demand".into(),
            ));
        }
        let generation = self.generation;
        let future = Rc::new(RefCell::new(Some(future.boxed_local())));
        let (abort, registration) = AbortHandle::new_pair();
        self.active.insert(
            agent.clone(),
            Owner {
                generation,
                future: future.clone(),
                abort,
            },
        );
        self.futures.push(Abortable::new(
            PendingSnapshot {
                agent,
                generation,
                future,
            },
            registration,
        ));
        Ok(())
    }
    pub async fn next(&mut self) -> Completed {
        // The driver polls this branch before each membership/layout event.
        // Retired I/O was already dropped by its indexed owner; drain only its
        // wake-queued shell, never scan other agents or outstanding requests.
        loop {
            match self.futures.next().await {
                Some(Ok(completed)) => return completed,
                Some(Err(_)) => {}
                None => return futures::future::pending().await,
            }
        }
    }
    pub fn finish(
        &mut self,
        completed: Completed,
    ) -> Option<(StreamPath, crate::task_volume::Result<HeadSnapshot>)> {
        let (agent, generation, snapshot) = completed;
        if self.active.get(&agent).map(|owner| owner.generation) != Some(generation) {
            return None;
        }
        self.active.remove(&agent);
        Some((agent, snapshot))
    }
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;

#[cfg(test)]
#[cfg(all(not(acyclic_harness_loom), not(target_arch = "wasm32")))]
mod retirement;
