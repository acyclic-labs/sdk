//! The order a sweep deletes unmarked objects in.
//!
//! A store refuses to delete an object while a parent still references it,
//! even a dead one. The sweep attempts every unmarked object once, parks
//! each refusal, and wakes a parked object only when a parent it names is
//! deleted. Each dead object is attempted at most once plus once per
//! parent, so one sweep reclaims a dead graph of any depth in work linear
//! in its size, and what stays parked is exactly what a root retains.

use std::collections::{HashSet, VecDeque};

/// The unmarked objects still to attempt, and the refusals awaiting a parent.
pub(crate) struct Worklist {
    queue: VecDeque<crate::Digest>,
    parked: HashSet<crate::Digest>,
}

impl Worklist {
    pub(crate) fn new(unmarked: Vec<crate::Digest>) -> Self {
        Self {
            queue: unmarked.into(),
            parked: HashSet::new(),
        }
    }

    /// Up to `limit` objects to attempt next; empty once nothing can progress.
    pub(crate) fn next(&mut self, limit: usize) -> Vec<crate::Digest> {
        let take = limit.min(self.queue.len());
        self.queue.drain(..take).collect()
    }

    /// Wakes every parked child of a deleted object.
    pub(crate) fn deleted(&mut self, children: impl IntoIterator<Item = crate::Digest>) {
        for child in children {
            if self.parked.remove(&child) {
                self.queue.push_back(child);
            }
        }
    }

    /// Parks an object some parent or root still references.
    pub(crate) fn refused(&mut self, digest: crate::Digest) {
        self.parked.insert(digest);
    }

    /// The objects a root still retains once the queue is empty.
    pub(crate) fn retained(&self) -> usize {
        self.parked.len()
    }
}

#[cfg(test)]
mod tests {
    use super::Worklist;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn a_dead_chain_against_digest_order_costs_its_nodes_plus_edges() {
        const DEPTH: u8 = 64;
        // Digest order lists the chain deepest first: every child is
        // attempted before the parent that pins it.
        let chain = (0..DEPTH)
            .rev()
            .map(|depth| crate::Digest::from_bytes([depth; 32]))
            .collect::<Vec<_>>();
        let child = chain
            .windows(2)
            .map(|pair| (pair[1], pair[0]))
            .collect::<HashMap<_, _>>();
        let mut parent = child
            .iter()
            .map(|(&parent, &child)| (child, parent))
            .collect::<HashMap<_, _>>();
        let mut stored = chain.iter().copied().collect::<HashSet<_>>();
        let mut work = Worklist::new(chain.clone());
        let mut attempts = 0;
        loop {
            let batch = work.next(4);
            if batch.is_empty() {
                break;
            }
            for digest in batch {
                attempts += 1;
                if parent.get(&digest).is_some_and(|p| stored.contains(p)) {
                    work.refused(digest);
                } else {
                    stored.remove(&digest);
                    parent.remove(&digest);
                    work.deleted(child.get(&digest).copied());
                }
            }
        }
        assert!(stored.is_empty(), "one sweep reclaims the whole chain");
        assert_eq!(work.retained(), 0);
        assert!(
            attempts <= usize::from(DEPTH) + child.len(),
            "{attempts} attempts for {DEPTH} nodes and {} edges",
            child.len()
        );
    }
}
