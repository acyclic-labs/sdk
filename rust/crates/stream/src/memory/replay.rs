use acyclic_collections::radix::{BinaryKey, RadixKey as _};

// Each branch is owned by its newly inserted original leaf. That leaf remains
// a representative of the subtree, including at a short request's boundary.
#[derive(Clone, Copy)]
struct ReplayBranch {
    bit: usize,
    left: RetainedIndexLink,
    right: RetainedIndexLink,
}

struct ReplayCursor<'a> {
    state: &'a State,
    link: Option<RetainedIndexLink>,
    parent: Option<(usize, bool)>,
}

impl RadixCursor for ReplayCursor<'_> {
    fn is_empty(&self) -> bool {
        self.link.is_none()
    }

    fn branch_bit(&self) -> Option<BranchBit> {
        match self.link {
            Some(RetainedIndexLink::Branch(index)) => Some(BranchBit::from_binary_bit(
                self.state.replay_branch(index).bit,
            )),
            _ => None,
        }
    }

    fn child(self, high: bool) -> Self {
        let Some(RetainedIndexLink::Branch(index)) = self.link else {
            unreachable!("selection descends from an existing branch")
        };
        let branch = self.state.replay_branch(index);
        Self {
            state: self.state,
            link: Some(if high { branch.right } else { branch.left }),
            parent: Some((index, high)),
        }
    }
}

impl State {
    // Links reference entries already in the append-only, originally funded inventory.
    fn replay_entry(&self, index: usize) -> &Retained {
        self.retained
            .get(index)
            .unwrap_or_else(|| unreachable!("retained replay link"))
    }

    fn replay_entry_mut(&mut self, index: usize) -> &mut Retained {
        self.retained
            .get_mut(index)
            .unwrap_or_else(|| unreachable!("retained replay link"))
    }

    fn replay_branch(&self, index: usize) -> &ReplayBranch {
        &self.replay_entry(index).replay_branch
    }

    fn replay_key(&self, index: usize) -> BinaryKey<'_> {
        BinaryKey(
            self.replay_entry(index)
                .replay
                .as_deref()
                .unwrap_or_else(|| unreachable!("retained replay identity")),
        )
    }

    fn replay_cursor(&self) -> ReplayCursor<'_> {
        ReplayCursor {
            state: self,
            link: self.replay_root,
            parent: None,
        }
    }

    fn replay<E>(
        &self,
        key: &[u8],
        mut probe: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<Option<&Retained>, E> {
        let key = BinaryKey(key);
        let (selected, _) = radix::select(self.replay_cursor(), &key, None, &mut probe)?;
        let Some(RetainedIndexLink::Leaf(index) | RetainedIndexLink::Branch(index)) = selected.link
        else {
            return Ok(None);
        };
        Ok(key
            .difference(self.replay_key(index), probe)?
            .is_none()
            .then(|| self.replay_entry(index)))
    }

    fn insert_replay(&mut self, index: usize, probe: &mut impl FnMut(u64)) {
        if self.replay_root.is_none() {
            self.replay_root = Some(RetainedIndexLink::Leaf(index));
            return;
        }
        probe(1);
        let key = self.replay_key(index);
        let mut observe = |items| {
            probe(items);
            Ok::<_, std::convert::Infallible>(())
        };
        let (selected, _) = radix::select(self.replay_cursor(), &key, None, &mut observe)
            .unwrap_or_else(|never| match never {});
        let Some(RetainedIndexLink::Leaf(other) | RetainedIndexLink::Branch(other)) = selected.link
        else {
            unreachable!("selection returns an original representative")
        };
        let bit = key
            .difference(self.replay_key(other), &mut observe)
            .unwrap_or_else(|never| match never {})
            .unwrap_or_else(|| unreachable!("duplicate retained replay identity"));
        let (selected, _) = radix::select(self.replay_cursor(), &key, Some(bit), &mut observe)
            .unwrap_or_else(|never| match never {});
        let next = selected
            .link
            .unwrap_or_else(|| unreachable!("nonempty selected subtree"));
        let parent = selected.parent;
        let leaf = RetainedIndexLink::Leaf(index);
        let (left, right) = if key
            .branch(bit)
            .unwrap_or_else(|| unreachable!("original differing bit"))
        {
            (next, leaf)
        } else {
            (leaf, next)
        };
        self.replay_entry_mut(index).replay_branch = ReplayBranch {
            bit: bit
                .binary_bit_index()
                .unwrap_or_else(|| unreachable!("replay coordinate admitted before publication")),
            left,
            right,
        };
        let link = RetainedIndexLink::Branch(index);
        if let Some((index, right)) = parent {
            let branch = &mut self.replay_entry_mut(index).replay_branch;
            if right {
                branch.right = link;
            } else {
                branch.left = link;
            }
        } else {
            self.replay_root = Some(link);
        }
    }
}

#[derive(Default)]
struct ReplayProbes {
    #[cfg(not(target_arch = "wasm32"))]
    meter: Option<Arc<dyn NativeReadMeter>>,
    #[cfg(not(target_arch = "wasm32"))]
    remaining: u64,
}

#[cfg(not(target_arch = "wasm32"))]
impl ReplayProbes {
    fn record(&mut self, items: u64) -> std::io::Result<()> {
        if let Some(meter) = &self.meter {
            self.remaining = self
                .remaining
                .checked_sub(items)
                .unwrap_or_else(|| unreachable!("reserved index examinations"));
            return meter.record_reserved_examination(items);
        }
        Ok(())
    }
}

impl Drop for ReplayProbes {
    fn drop(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = &self.meter {
            meter.cancel_reserved_examination(self.remaining);
        }
    }
}

impl MemoryStream {
    fn record_replay_probe(&self, probes: &mut ReplayProbes, items: u64) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            let _ = work.record(probes.record(items));
        }
        let _ = (probes, items);
    }
    fn replay_insertion_probes(&self, key: &[u8]) -> Result<ReplayProbes, StreamError> {
        self.reserve_replay_probes(
            BinaryKey(key)
                .insertion_examinations()
                .ok_or(StreamError::Capacity)?,
        )
    }

    fn replay<'a>(
        &self,
        state: &'a State,
        key: &[u8],
    ) -> Result<Option<&'a Retained>, StreamError> {
        state.replay(key, |items| {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = &self.work {
                work.record(work.meter.examine(items, 0))?;
            }
            let _ = items;
            Ok(())
        })
    }

    fn reserve_replay_probes(&self, remaining: u64) -> Result<ReplayProbes, StreamError> {
        if remaining == 0 {
            return Ok(ReplayProbes::default());
        }
        let _ = remaining;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.reserve_examination(remaining))?;
        }
        Ok(ReplayProbes {
            #[cfg(not(target_arch = "wasm32"))]
            meter: self.work.as_ref().map(|work| Arc::clone(&work.meter)),
            #[cfg(not(target_arch = "wasm32"))]
            remaining,
        })
    }
}
