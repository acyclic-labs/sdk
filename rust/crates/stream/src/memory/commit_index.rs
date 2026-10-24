// Derived fixed-width index into the originally funded retained inventory.
//
// A Patricia branch belongs to the newly inserted leaf's Retained slot, so
// indexing never allocates or grows a second inventory after publication.
#[cfg(not(target_arch = "wasm32"))]
const COMMIT_INDEX_PROBES: u64 = 2 * 257;

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

fn commit_bit(key: &[u8; 32], bit: usize) -> bool {
    key.get(bit / 8)
        .is_some_and(|byte| byte & (0x80 >> (bit % 8)) != 0)
}

fn commit_difference(left: &[u8; 32], right: &[u8; 32]) -> Option<usize> {
    for (byte, (left, right)) in left.iter().zip(right).enumerate() {
        let different = left ^ right;
        for bit in 0..8 {
            if different & (0x80 >> bit) != 0 {
                return Some(byte * 8 + bit);
            }
        }
    }
    None
}

impl CommitBranch {
    fn child(self, key: &[u8; 32]) -> CommitLink {
        if commit_bit(key, self.bit) {
            self.right
        } else {
            self.left
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

    fn find_commit<E>(
        &self,
        id: &CommitId,
        mut probe: impl FnMut() -> Result<(), E>,
    ) -> Result<Option<&CommittedEnvelope>, E> {
        let mut next = self.commit_root;
        while let Some(link) = next {
            probe()?;
            #[cfg(test)]
            COMMIT_VISITS.with(|count| count.set(count.get() + 1));
            match link {
                CommitLink::Leaf(index) => {
                    return Ok(self
                        .replay_entry(index)
                        .envelope()
                        .filter(|envelope| envelope.commit_id == *id));
                }
                CommitLink::Branch(index) => {
                    next = Some(self.commit_branch(index).child(id.as_bytes()));
                }
            }
        }
        Ok(None)
    }

    fn insert_commit(&mut self, index: usize, probe: &mut impl FnMut()) {
        let Some(root) = self.commit_root else {
            self.commit_root = Some(CommitLink::Leaf(index));
            return;
        };
        probe();
        let key = self.commit_identity(index);
        let mut next = root;
        let other = loop {
            probe();
            match next {
                CommitLink::Leaf(index) => break self.commit_identity(index),
                CommitLink::Branch(index) => next = self.commit_branch(index).child(key.as_bytes()),
            }
        };
        let bit = commit_difference(key.as_bytes(), other.as_bytes())
            .unwrap_or_else(|| unreachable!("duplicate retained commit identity"));
        let mut parent = None;
        next = root;
        loop {
            probe();
            match next {
                CommitLink::Branch(index) if self.commit_branch(index).bit < bit => {
                    let branch = *self.commit_branch(index);
                    parent = Some((index, commit_bit(key.as_bytes(), branch.bit)));
                    next = branch.child(key.as_bytes());
                }
                _ => break,
            }
        }
        let leaf = CommitLink::Leaf(index);
        let (left, right) = if commit_bit(key.as_bytes(), bit) {
            (next, leaf)
        } else {
            (leaf, next)
        };
        self.replay_entry_mut(index).commit_branch = Some(CommitBranch { bit, left, right });
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
        // caller budget exhaustion. Reserve both complete 256-bit walks now;
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
