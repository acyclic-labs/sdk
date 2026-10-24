use super::PathTrie;

impl PathTrie {
    pub(in crate::access) fn subtree_action_mask(&self, prefix: &str, now: u64) -> u16 {
        let Ok(path) = self.prepare(prefix) else {
            return 0;
        };
        if !self.valid_at(now) {
            return 0;
        }
        let mut actions = self.action_mask(path.state, now);
        let start = if prefix.is_empty() {
            path.state
        } else {
            let Ok(state) = self.step(path.state, '/') else {
                return actions;
            };
            state
        };
        let mut seen = vec![false; self.state_count()];
        let mut pending = vec![start];
        let symbols: Vec<_> = self
            .alphabet
            .symbols
            .iter()
            .copied()
            .chain(
                self.shared
                    .iter()
                    .flat_map(|binding| binding.policy.alphabet.symbols.iter().copied()),
            )
            .chain(std::iter::once('/'))
            .collect();
        let Some(other) = (1..=0x10ffff)
            .filter_map(char::from_u32)
            .find(|ch| !symbols.contains(ch))
        else {
            return actions;
        };
        let Some(initial) = seen.get_mut(start) else {
            return 0;
        };
        *initial = true;
        while let Some(state) = pending.pop() {
            actions |= self.action_mask(state, now);
            for ch in symbols.iter().copied().chain(std::iter::once(other)) {
                let Ok(next) = self.step(state, ch) else {
                    continue;
                };
                let Some(visited) = seen.get_mut(next) else {
                    return 0;
                };
                if !*visited {
                    *visited = true;
                    pending.push(next);
                }
            }
        }
        actions
    }
}
