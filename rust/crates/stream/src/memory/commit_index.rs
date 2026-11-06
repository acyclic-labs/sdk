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
enum CommitLink {
    Leaf(usize),
    Branch(usize),
}

#[derive(Clone, Copy)]
struct CommitBranch {
    bit: usize,
    left: CommitLink,
    right: CommitLink,
}

// This adapter borrows only the original inventory. Selection and bit/key
// interpretation are the same implementation used by the boxed FixedIndex.
struct CommitCursor<'a> {
    state: &'a State,
    link: Option<CommitLink>,
    parent: Option<(usize, bool)>,
}

impl RadixCursor for CommitCursor<'_> {
    fn is_empty(&self) -> bool {
        self.link.is_none()
    }

    fn branch_bit(&self) -> Option<BranchBit> {
        match self.link {
            Some(CommitLink::Branch(index)) => {
                Some(BranchBit::from_bit(self.state.commit_branch(index).bit))
            }
            _ => None,
        }
    }

    fn child(self, high: bool) -> Self {
        let Some(CommitLink::Branch(index)) = self.link else {
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
        mut probe: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<&CommittedEnvelope>, E> {
        let mut observe = |items| {
            probe(items)?;
            #[cfg(test)]
            COMMIT_VISITS.with(|count| count.set(count.get() + items as usize));
            Ok(())
        };
        let (selected, _) = radix::select(self.commit_cursor(), id.as_bytes(), None, &mut observe)?;
        match selected.link {
            Some(CommitLink::Leaf(index)) => {
                observe(32)?;
                Ok(self
                    .replay_entry(index)
                    .envelope()
                    .filter(|envelope| envelope.commit_id == *id))
            }
            None => Ok(None),
            _ => unreachable!("selection returns a retained commit leaf"),
        }
    }

    fn insert_commit(&mut self, index: usize, probe: &mut impl FnMut(u64)) {
        if self.commit_root.is_none() {
            self.commit_root = Some(CommitLink::Leaf(index));
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
        let Some(CommitLink::Leaf(other)) = selected.link else {
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
        let leaf = CommitLink::Leaf(index);
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
        let link = CommitLink::Branch(index);
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
        if let Some(work) = &self.work {
            let remaining = probes
                .remaining
                .checked_add(COMMIT_INDEX_PROBES)
                .ok_or(StreamError::Capacity)?;
            work.record(work.meter.reserve_examination(COMMIT_INDEX_PROBES))?;
            probes.remaining = remaining;
            probes.meter = Some(Arc::clone(&work.meter));
        }
        let _ = probes;
        Ok(())
    }
}
