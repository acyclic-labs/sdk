use super::*;

impl Markdown {
    pub(super) fn advance_budget(&mut self) -> usize {
        if self.source.len().max(100_000) == self.reference_limit {
            return self.visible_lines;
        }
        let changed = self.resolve(&HashSet::new());
        let remaining = self.reference_limit.saturating_sub(self.reference_cost);
        if self
            .inline
            .as_ref()
            .is_some_and(|inline| !inline.checkpoint.valid.contains(&remaining))
            || self
                .table
                .as_ref()
                .is_some_and(|table| !table.valid.contains(&remaining))
        {
            self.inline = None;
            self.table = None;
            self.plain = false;
        }
        self.finish_visible();
        changed
    }

    pub(super) fn render_cache(
        &self,
        cache: &mut Cached,
        out: &mut render::Renderer,
        cost: &mut usize,
    ) {
        if cache
            .template
            .keys
            .iter()
            .all(|key| self.references.get(key).is_none())
        {
            return;
        }
        let (events, used, interval) = cache.template.events(
            &self.references,
            self.source.len().max(100_000).saturating_sub(*cost),
        );
        cache.cost = used;
        cache.budget = interval;
        *cost = cost.saturating_add(used);
        let mut replacement = cache.state.checkpoint();
        if !cache.lines.is_empty()
            && let Some(line) = out.lines.get_mut(cache.lines.start - self.lines.len())
        {
            #[cfg(test)]
            work::copy(std::mem::size_of::<Line>());
            replacement.reuse_line(std::mem::take(line));
        }
        for event in &events {
            replacement.event_in(event, Some(&self.references));
        }
        replacement.flush();
        out.lines.truncate(cache.lines.start - self.lines.len());
        out.lines.extend(replacement.lines);
        cache.lines.end = self.lines.len() + out.lines.len();
    }

    pub(super) fn resolve(&mut self, keys: &HashSet<Key>) -> usize {
        let mut affected: HashSet<usize> = HashSet::new();
        for key in keys {
            if let Some(indices) = self.dependencies.get(key) {
                affected.extend(indices);
            }
        }
        let limit = self.source.len().max(100_000);
        let old_limit = self.reference_limit;
        self.reference_limit = limit;
        let budget_first = if limit != old_limit && self.reference_cost > limit.min(old_limit) {
            self.cached
                .partition_point(|cache| cache.cumulative < limit.min(old_limit))
        } else {
            self.cached.len()
        };
        let first = affected
            .iter()
            .copied()
            .min()
            .unwrap_or(self.cached.len())
            .min(budget_first);
        if first == self.cached.len() {
            return self.visible_lines;
        }
        let mut cost = first
            .checked_sub(1)
            .and_then(|index| self.cached.get(index))
            .map_or(0, |cache| cache.cumulative);
        let mut changed = self.visible_lines;
        let mut replacements = HashMap::new();
        let mut length = self.lines.len();
        let mut resize = false;
        let mut complete = true;
        for index in first..self.cached.len() {
            let Some(cache) = self.cached.get_mut(index) else {
                continue;
            };
            let old_prefix = cache.prefix;
            if affected.is_empty() && cost >= limit && old_prefix >= old_limit {
                complete = false;
                break;
            }
            #[cfg(test)]
            {
                self.reference_cache_visits += 1;
            }
            let budget = limit.saturating_sub(cost);
            let same = cache.budget.contains(&budget);
            if budget == 0 && cache.budget.start == 0 {
                // No reference was resolved before, and none can resolve now.
                // Retain its literal lines until allowance reaches this block.
                cache.cost = usize::MAX;
                cache.budget = 0..1;
            } else if affected.contains(&index) || !same {
                let mut renderer = cache.state.checkpoint();
                if !cache.lines.is_empty()
                    && let Some(line) = self.lines.get_mut(cache.lines.start)
                {
                    #[cfg(test)]
                    work::copy(std::mem::size_of::<Line>());
                    renderer.reuse_line(std::mem::take(line));
                }
                let (events, used, interval) = cache.template.events(&self.references, budget);
                cache.cost = used;
                cache.budget = interval;
                for event in events {
                    renderer.event_in(&event, Some(&self.references));
                }
                renderer.flush();
                changed = changed.min(cache.lines.start);
                length = length - cache.lines.len() + renderer.lines.len();
                resize |= cache.lines.len() != renderer.lines.len();
                replacements.insert(index, renderer.lines);
            }
            cache.prefix = cost;
            cost = cost.saturating_add(cache.cost);
            cache.cumulative = cost;
        }
        if complete {
            self.reference_cost = cost;
        }
        self.apply_reference_lines(replacements, length, resize);
        changed
    }

    fn apply_reference_lines(
        &mut self,
        replacements: HashMap<usize, Vec<Line>>,
        length: usize,
        resize: bool,
    ) {
        if resize {
            self.replace_reference_lines(replacements, length);
        } else {
            for (index, lines) in replacements {
                #[cfg(test)]
                {
                    self.reference_cache_visits += 1;
                    self.reference_moved_lines += lines.len();
                    work::copy(lines.len() * std::mem::size_of::<Line>());
                }
                if let Some(cache) = self.cached.get(index) {
                    for (target, line) in self
                        .lines
                        .get_mut(cache.lines.clone())
                        .unwrap_or_default()
                        .iter_mut()
                        .zip(lines)
                    {
                        *target = line;
                    }
                    let range = cache.lines.clone();
                    for index in range {
                        self.index_visible(index);
                    }
                }
            }
        }
    }

    fn replace_reference_lines(
        &mut self,
        mut replacements: HashMap<usize, Vec<Line>>,
        length: usize,
    ) {
        let first_cache = replacements.keys().copied().min().unwrap_or_default();
        let first_line = self
            .cached
            .get(first_cache)
            .map_or(0, |cache| cache.lines.start);
        let previous_length = self.lines.len();
        let previous_stable = self.stable_lines;
        #[cfg(test)]
        work::copy((previous_length - first_line) * std::mem::size_of::<Line>());
        let mut previous = self.lines.split_off(first_line).into_iter();
        let mut cursor = first_line;
        let mut lines = std::mem::take(&mut self.lines);
        #[cfg(test)]
        if length > lines.capacity() {
            work::copy(lines.len() * std::mem::size_of::<Line>());
        }
        lines.reserve(length.saturating_sub(lines.len()));
        // Caches are appended in source order. Walk them once while consuming
        // each old line once; neither splice a suffix nor rescan cache ranges.
        for (index, cache) in self.cached.iter_mut().enumerate().skip(first_cache) {
            #[cfg(test)]
            {
                self.reference_cache_visits += 1;
            }
            let range = cache.lines.clone();
            if let Some(replacement) = replacements.remove(&index) {
                lines.extend(previous.by_ref().take(range.start - cursor));
                previous.by_ref().take(range.len()).for_each(drop);
                cursor = range.end;
                cache.lines.start = lines.len();
                lines.extend(replacement);
                cache.lines.end = lines.len();
            } else {
                let start = range.start - cursor + lines.len();
                cache.lines = start..start + range.len();
            }
        }
        lines.extend(previous);
        #[cfg(test)]
        {
            self.reference_moved_lines += lines.len() - first_line;
            work::copy((lines.len() - first_line) * std::mem::size_of::<Line>());
        }
        self.stable_lines = self.stable_lines + lines.len() - previous_length;
        self.lines = lines;
        for index in self.stable_lines..previous_stable {
            self.visibility.set(index, false);
        }
        for index in first_line..self.stable_lines {
            self.index_visible(index);
        }
    }
}
