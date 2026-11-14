// Derived fixed-width index into the originally funded retained inventory.
//
// A Patricia branch belongs to the newly inserted leaf's Retained slot, so
// indexing never allocates or grows a second inventory after publication.
use acyclic_collections::radix::{self, BranchBit, RadixCursor};

// The shared insertion bound plus the original new-leaf identity observation.
#[cfg(not(target_arch = "wasm32"))]
const COMMIT_INDEX_PROBES: u64 = match radix::insertion_examinations(32) {
    Some(bound) => bound + 1,
    None => unreachable!(),
};

#[derive(Clone, Copy)]
enum RetainedIndexLink {
    Leaf(usize),
    Branch(usize),
}

#[derive(Clone, Copy)]
struct CommitBranch {
    bit: usize,
    left: RetainedIndexLink,
    right: RetainedIndexLink,
}

// This adapter borrows only the original inventory. Selection and bit/key
// interpretation are the same implementation used by the boxed FixedIndex.
struct CommitCursor<'a> {
    state: &'a State,
    link: Option<RetainedIndexLink>,
    parent: Option<(usize, bool)>,
}

impl RadixCursor for CommitCursor<'_> {
    fn is_empty(&self) -> bool {
        self.link.is_none()
    }

    fn branch_bit(&self) -> Option<BranchBit> {
        match self.link {
            Some(RetainedIndexLink::Branch(index)) => {
                Some(BranchBit::from_bit(self.state.commit_branch(index).bit))
            }
            _ => None,
        }
    }

    fn child(self, high: bool) -> Self {
        let Some(RetainedIndexLink::Branch(index)) = self.link else {
            unreachable!("selection descends from an existing branch")
        };
        let branch = self.state.commit_branch(index);
        Self {
            state: self.state,
            link: Some(if high { branch.right } else { branch.left }),
            parent: Some((index, high)),
        }
    }
}

impl State {
    fn commit_branch(&self, index: usize) -> &CommitBranch {
        self.replay_entry(index)
            .commit_branch
            .as_ref()
            .unwrap_or_else(|| unreachable!("retained commit branch"))
    }

    fn commit_identity(&self, index: usize) -> CommitId {
        self.replay_entry(index)
            .envelope()
            .unwrap_or_else(|| unreachable!("retained commit leaf"))
            .commit_id
    }

    fn commit_cursor(&self) -> CommitCursor<'_> {
        CommitCursor {
            state: self,
            link: self.commit_root,
            parent: None,
        }
    }

    fn find_commit<E>(
        &self,
        id: &CommitId,
        probe: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<&CommittedEnvelope>, E> {
        Ok(self
            .find_commit_fact(id, probe)?
            .and_then(RetainedFact::envelope))
    }

    fn find_commit_fact<E>(
        &self,
        id: &CommitId,
        mut probe: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<&RetainedFact>, E> {
        let mut observe = |items| {
            probe(items)?;
            #[cfg(test)]
            COMMIT_VISITS.with(|count| count.set(count.get() + items as usize));
            Ok(())
        };
        let (selected, _) = radix::select(self.commit_cursor(), id.as_bytes(), None, &mut observe)?;
        let durable = match selected.link {
            Some(RetainedIndexLink::Leaf(index)) => {
                observe(32)?;
                let fact = &*self.replay_entry(index).fact;
                fact
                    .envelope()
                    .filter(|envelope| envelope.commit_id == *id)
                    .map(|_| fact)
            }
            None => None,
            _ => unreachable!("selection returns a retained commit leaf"),
        };
        if durable.is_some() || self.ephemeral.roots.is_empty() {
            return Ok(durable);
        }
        Ok(self
            .ephemeral
            .receipts
            .commits
            .get(*id.as_bytes(), &mut ReceiptProbe(observe))?
            .filter(|fact| fact.is_live())
            .map(|receipt| &*receipt.fact))
    }

    fn insert_commit(&mut self, index: usize, probe: &mut impl FnMut(u64)) {
        if self.commit_root.is_none() {
            self.commit_root = Some(RetainedIndexLink::Leaf(index));
            return;
        }
        probe(1);
        let key = self.commit_identity(index);
        let mut observe = |items| {
            probe(items);
            Ok::<_, std::convert::Infallible>(())
        };
        let (selected, _) =
            match radix::select(self.commit_cursor(), key.as_bytes(), None, &mut observe) {
                Ok(selected) => selected,
                Err(never) => match never {},
            };
        let Some(RetainedIndexLink::Leaf(other)) = selected.link else {
            unreachable!("selection returns a retained commit leaf")
        };
        observe(32).unwrap_or_else(|never| match never {});
        let bit = BranchBit::difference(key.as_bytes(), self.commit_identity(other).as_bytes())
            .unwrap_or_else(|| unreachable!("duplicate retained commit identity"));
        let (selected, _) = match radix::select(
            self.commit_cursor(),
            key.as_bytes(),
            Some(bit),
            &mut observe,
        ) {
            Ok(selected) => selected,
            Err(never) => match never {},
        };
        let next = selected
            .link
            .unwrap_or_else(|| unreachable!("a selected subtree is nonempty"));
        let parent = selected.parent;
        let leaf = RetainedIndexLink::Leaf(index);
        let (left, right) = if bit.high(key.as_bytes()) {
            (next, leaf)
        } else {
            (leaf, next)
        };
        self.replay_entry_mut(index).commit_branch = Some(CommitBranch {
            bit: bit
                .bit_index()
                .unwrap_or_else(|| unreachable!("a commit key has 256 bits")),
            left,
            right,
        });
        let link = RetainedIndexLink::Branch(index);
        if let Some((index, right)) = parent {
            let branch = self
                .replay_entry_mut(index)
                .commit_branch
                .as_mut()
                .unwrap_or_else(|| unreachable!("retained commit parent"));
            if right {
                branch.right = link;
            } else {
                branch.left = link;
            }
        } else {
            self.commit_root = Some(link);
        }
    }
}

impl MemoryStream {
    fn reserve_commit_index(&self, probes: &mut ReplayProbes) -> Result<(), StreamError> {
        // Prepared completion can run after other commits, cancellation and
        // caller budget exhaustion. Reserve shared traversal and complete key comparison now;
        // retain only records already admitted probes and refunds the unused tail.
        #[cfg(not(target_arch = "wasm32"))]
        self.extend_replay_probes(probes, COMMIT_INDEX_PROBES)?;
        let _ = probes;
        Ok(())
    }
}
