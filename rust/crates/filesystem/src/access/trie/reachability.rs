use super::{Action, PathTrie, PreparedPath};

/// How completely a subtree walk visited the reachable automaton states.
enum Walk {
    /// Every state reachable below the prefix was visited.
    Complete,
    /// Some transition could not be followed; only visited states count.
    Partial,
    /// The view is invalid at this time.
    Invalid,
}

impl PathTrie {
    /// Union of the actions any path at or below `prefix` permits.
    pub(in crate::access) fn subtree_action_mask(&self, prefix: &str, now: u64) -> u16 {
        let Ok(path) = self.prepare(prefix) else {
            return 0;
        };
        let mut actions = 0;
        match self.walk_subtree(path, now, &mut |mask| actions |= mask) {
            Walk::Invalid => 0,
            Walk::Complete | Walk::Partial => actions,
        }
    }

    /// Proof that `prefix` and every path below it permit each of `actions`,
    /// including capability intersections. Paths outside the subtree do not
    /// matter. An incomplete walk or an invalid view proves nothing.
    #[must_use]
    pub fn allows_subtree(&self, prefix: &str, actions: &[Action], now: u64) -> bool {
        self.prepare(prefix)
            .is_ok_and(|path| self.allows_prepared_subtree(path, actions, now))
    }

    #[cfg(all(not(target_arch = "wasm32"), any(windows, test)))]
    pub(crate) fn allows_native_subtree(
        &self,
        prefix: &crate::kernel::NamespacePath,
        actions: &[Action],
        now: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, crate::NativeTreeMaterializationError> {
        if prefix.is_root() {
            return Ok(self.allows_every_path(actions, now));
        }
        let Ok(path) = self.prepare_namespace(prefix) else {
            return Ok(false);
        };
        let bytes = self
            .state_count()
            .checked_mul(std::mem::size_of::<usize>() + std::mem::size_of::<bool>())
            .zip(
                self.subtree_symbol_count()
                    .checked_mul(std::mem::size_of::<char>()),
            )
            .and_then(|(states, symbols)| states.checked_add(symbols))
            .ok_or(crate::WorkError::Overflow)?;
        let _scratch = work.claim(bytes as u64, 3, crate::WorkCounters::UNCHARGED)?;
        Ok(self.allows_prepared_subtree(path, actions, now))
    }

    fn allows_prepared_subtree(
        &self,
        path: PreparedPath<'_>,
        actions: &[Action],
        now: u64,
    ) -> bool {
        let required = actions
            .iter()
            .fold(0u16, |mask, action| mask | 1 << *action as u16);
        let mut common = u16::MAX;
        matches!(
            self.walk_subtree(path, now, &mut |mask| common &= mask),
            Walk::Complete
        ) && common & required == required
    }

    /// Visits the action mask of `prefix` and of each automaton state reachable
    /// below it once.
    fn walk_subtree(&self, path: PreparedPath<'_>, now: u64, visit: &mut dyn FnMut(u16)) -> Walk {
        if !self.valid_at(now) {
            return Walk::Invalid;
        }
        visit(self.action_mask(path.state, now));
        let start = if path.depth == 0 {
            path.state
        } else {
            let Ok(state) = self.step(path.state, '/') else {
                return Walk::Partial;
            };
            state
        };
        let mut seen = vec![false; self.state_count()];
        let mut pending = Vec::with_capacity(self.state_count());
        pending.push(start);
        let mut symbols = Vec::with_capacity(self.subtree_symbol_count());
        symbols.extend_from_slice(&self.alphabet.symbols);
        if let Some(binding) = &self.shared {
            symbols.extend_from_slice(&binding.policy.alphabet.symbols);
        }
        symbols.push('/');
        let Some(other) = (1..=0x10ffff)
            .filter_map(char::from_u32)
            .find(|ch| !symbols.contains(ch))
        else {
            return Walk::Partial;
        };
        let Some(initial) = self
            .state_index(start)
            .and_then(|index| seen.get_mut(index))
        else {
            return Walk::Invalid;
        };
        *initial = true;
        let mut walk = Walk::Complete;
        while let Some(state) = pending.pop() {
            visit(self.action_mask(state, now));
            for ch in symbols.iter().copied().chain(std::iter::once(other)) {
                let Ok(next) = self.step(state, ch) else {
                    walk = Walk::Partial;
                    continue;
                };
                let Some(visited) = self.state_index(next).and_then(|index| seen.get_mut(index))
                else {
                    return Walk::Invalid;
                };
                if !*visited {
                    *visited = true;
                    pending.push(next);
                }
            }
        }
        walk
    }

    fn subtree_symbol_count(&self) -> usize {
        self.alphabet.symbols.len()
            + self
                .shared
                .as_ref()
                .map_or(0, |binding| binding.policy.alphabet.symbols.len())
            + 1
    }
}
