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

    fn replay_height(&self, index: Option<usize>) -> u8 {
        index.map_or(0, |index| self.replay_entry(index).replay_height)
    }

    fn update_replay_height(&mut self, index: usize) {
        let entry = self.replay_entry(index);
        let height = 1 + self
            .replay_height(entry.replay_left)
            .max(self.replay_height(entry.replay_right));
        self.replay_entry_mut(index).replay_height = height;
    }

    fn rotate_replay_left(&mut self, root: usize) -> usize {
        let Some(right) = self.replay_entry(root).replay_right else {
            return root;
        };
        self.replay_entry_mut(root).replay_right = self.replay_entry(right).replay_left;
        self.replay_entry_mut(right).replay_left = Some(root);
        self.update_replay_height(root);
        self.update_replay_height(right);
        right
    }

    fn rotate_replay_right(&mut self, root: usize) -> usize {
        let Some(left) = self.replay_entry(root).replay_left else {
            return root;
        };
        self.replay_entry_mut(root).replay_left = self.replay_entry(left).replay_right;
        self.replay_entry_mut(left).replay_right = Some(root);
        self.update_replay_height(root);
        self.update_replay_height(left);
        left
    }

    fn insert_replay(
        &mut self,
        root: Option<usize>,
        index: usize,
        probe: &mut impl FnMut(),
    ) -> usize {
        let Some(root) = root else { return index };
        probe();
        if self.replay_entry(index).replay.as_deref() < self.replay_entry(root).replay.as_deref() {
            let left = self.insert_replay(self.replay_entry(root).replay_left, index, probe);
            self.replay_entry_mut(root).replay_left = Some(left);
        } else {
            let right = self.insert_replay(self.replay_entry(root).replay_right, index, probe);
            self.replay_entry_mut(root).replay_right = Some(right);
        }
        self.update_replay_height(root);
        let entry = self.replay_entry(root);
        let left = self.replay_height(entry.replay_left);
        let right = self.replay_height(entry.replay_right);
        if left > right + 1 {
            if let Some(child) = entry.replay_left {
                let entry = self.replay_entry(child);
                if self.replay_height(entry.replay_right) > self.replay_height(entry.replay_left) {
                    let child = self.rotate_replay_left(child);
                    self.replay_entry_mut(root).replay_left = Some(child);
                }
            }
            self.rotate_replay_right(root)
        } else if right > left + 1 {
            if let Some(child) = entry.replay_right {
                let entry = self.replay_entry(child);
                if self.replay_height(entry.replay_left) > self.replay_height(entry.replay_right) {
                    let child = self.rotate_replay_right(child);
                    self.replay_entry_mut(root).replay_right = Some(child);
                }
            }
            self.rotate_replay_left(root)
        } else {
            root
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
                .unwrap_or_else(|| unreachable!("reserved AVL height"));
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
    fn record_replay_probe(&self, probes: &mut ReplayProbes) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            let _ = work.record(probes.record(1));
        }
        let _ = probes;
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn maximum_replay_height(&self) -> u64 {
        2 * u64::from(self.limits.idempotency_results.saturating_add(1).ilog2() + 1)
    }

    fn replay<'a>(
        &self,
        state: &'a State,
        key: &[u8],
    ) -> Result<Option<&'a Retained>, StreamError> {
        state.replay(key, || {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = &self.work {
                work.record(work.meter.examine(1, 0))?;
            }
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
