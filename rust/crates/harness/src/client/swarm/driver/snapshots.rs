//! Bounded snapshot I/O polled beside demand and followed records by one owner.
use super::*;
use futures::{StreamExt as _, future::LocalBoxFuture, stream::FuturesUnordered};
use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context, Poll},
};

type Completed = (StreamPath, u64, crate::task_volume::Result<HeadSnapshot>);
struct PendingSnapshot<'a> {
    agent: StreamPath,
    generation: u64,
    future: LocalBoxFuture<'a, crate::task_volume::Result<HeadSnapshot>>,
}
impl Future for PendingSnapshot<'_> {
    type Output = Completed;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Completed> {
        self.future
            .as_mut()
            .poll(cx)
            .map(|result| (self.agent.clone(), self.generation, result))
    }
}
pub(super) struct Snapshots<'a> {
    generation: u64,
    maximum: usize,
    active: BTreeMap<StreamPath, u64>,
    futures: FuturesUnordered<PendingSnapshot<'a>>,
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
        // Requeue retained pinned futures, and physically drop obsolete I/O now.
        // An abort-only queue could retain cancelled requests behind repeated
        // ready layout writes. Here both maps are always bounded by selection.
        self.futures = std::mem::take(&mut self.futures)
            .into_iter()
            .filter(|pending| self.active.get(&pending.agent) == Some(&pending.generation))
            .collect();
        Ok(())
    }
    pub fn retire(&mut self, agent: &StreamPath) -> Result<()> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("snapshot generation overflow".into()))?;
        self.active.remove(agent);
        self.futures = std::mem::take(&mut self.futures)
            .into_iter()
            .filter(|pending| &pending.agent != agent)
            .collect();
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
        self.active.insert(agent.clone(), generation);
        self.futures.push(PendingSnapshot {
            agent,
            generation,
            future: future.boxed_local(),
        });
        Ok(())
    }
    pub async fn next(&mut self) -> Completed {
        match self.futures.next().await {
            Some(completed) => completed,
            None => futures::future::pending().await,
        }
    }
    pub fn finish(
        &mut self,
        completed: Completed,
    ) -> Option<(StreamPath, crate::task_volume::Result<HeadSnapshot>)> {
        let (agent, generation, snapshot) = completed;
        if self.active.get(&agent) != Some(&generation) {
            return None;
        }
        self.active.remove(&agent);
        Some((agent, snapshot))
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
