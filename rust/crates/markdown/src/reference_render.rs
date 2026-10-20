use super::*;

type Candidate = (Range<usize>, Range<usize>, render::Renderer, Range<usize>);

impl Markdown {
    pub(super) fn cache_frontier(
        &mut self,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        calls: &[(usize, usize)],
        candidates: Vec<Candidate>,
        (base, prefix): (usize, usize),
    ) {
        let mut charges = calls.iter().peekable();
        for (range, lines, state, window) in candidates {
            if base + range.end.saturating_sub(prefix) > self.offset {
                #[cfg(test)]
                if COMPILE_PROVISIONAL.get() {
                    Cached::new(
                        source,
                        range,
                        lines,
                        state,
                        events.get(window).unwrap_or_default(),
                    );
                }
                break;
            }
            let Some(mut cache) = Cached::new(
                source,
                range.clone(),
                lines,
                state,
                events.get(window).unwrap_or_default(),
            ) else {
                continue;
            };
            (cache.cost, cache.budget) = references::expansion(
                std::iter::from_fn(|| {
                    #[cfg(test)]
                    work::scan(1);
                    if charges.peek().is_some_and(|(end, _)| *end <= range.end) {
                        charges.next()
                    } else {
                        None
                    }
                })
                .filter(|(end, _)| *end > range.start),
            );
            self.cache(cache);
        }
    }

    pub(super) fn advance_budget(&mut self) -> usize {
        if self.source.len().max(100_000) == self.reference_limit {
            return self.visible_lines;
        }
        let limit = self.source.len().max(100_000);
        // Growth can only reopen cached blocks after an exhausted expansion
        // budget. Otherwise their output and its visibility stay unchanged.
        let reopen = self.reference_cost > self.reference_limit.min(limit);
        #[cfg(test)]
        let reopen = reopen || RESCAN_BUDGET.get();
        let changed = if reopen {
            self.resolve(&HashSet::new())
        } else {
            self.reference_limit = limit;
            self.visible_lines
        };
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
        if reopen {
            self.finish_visible();
        }
        changed
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
        let mut growing = false;
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
                growing |= renderer.lines.len() > cache.lines.len();
                replacements.insert(index, renderer.lines);
            }
            cache.prefix = cost;
            cost = cost.saturating_add(cache.cost);
            cache.cumulative = cost;
        }
        if complete {
            self.reference_cost = cost;
        }
        // Invalidate the alternate before any open-block preview can swap it
        // back over reference replacements, including budget-only updates.
        self.spare.invalidate(changed);
        self.apply_reference_lines(replacements, length, resize, growing);
        changed
    }

    fn apply_reference_lines(
        &mut self,
        replacements: HashMap<usize, Vec<Line>>,
        length: usize,
        resize: bool,
        growing: bool,
    ) {
        let compact = resize && !growing;
        #[cfg(test)]
        let compact = compact && !COPY_SUFFIX.get();
        if compact {
            self.compact_reference_lines(replacements);
        } else if resize {
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
                    for (index, line) in cache.lines.clone().zip(lines) {
                        if let Some(row) = self.lines.get_mut(index) {
                            *row = line;
                        }
                    }
                    let range = cache.lines.clone();
                    for index in range {
                        self.index_visible(index);
                    }
                }
            }
        }
    }

    fn compact_reference_lines(&mut self, mut replacements: HashMap<usize, Vec<Line>>) {
        let Some(first) = replacements.keys().copied().min() else {
            return;
        };
        let Some(cache) = self.cached.get(first) else {
            return;
        };
        let first_line = cache.lines.start;
        let previous_length = self.lines.len();
        let previous_stable = self.stable_lines;
        let mut caches = self.cached.iter_mut().enumerate().skip(first).peekable();
        let (mut read, mut write) = (first_line, first_line);
        #[cfg(test)]
        let mut moved = 0;
        let mut replacement = None::<(Range<usize>, std::vec::IntoIter<Line>)>;
        // Compact the affected suffix in place. Each surviving old line moves
        // at most once; no temporary copy of the entire suffix is necessary.
        self.lines.retain_suffix(first_line, |line| {
            while caches
                .peek()
                .is_some_and(|(_, cache)| cache.lines.start <= read)
            {
                let Some((index, cache)) = caches.next() else {
                    break;
                };
                #[cfg(test)]
                {
                    self.reference_cache_visits += 1;
                }
                let old = cache.lines.clone();
                let lines = replacements.remove(&index);
                let length = lines.as_ref().map_or(old.len(), Vec::len);
                let start = write + old.start - read;
                cache.lines = start..start + length;
                if !old.is_empty() {
                    replacement = lines.map(|lines| (old, lines.into_iter()));
                }
            }
            #[cfg(test)]
            work::scan(1);
            #[cfg(test)]
            let mut replaced = false;
            let remove = if let Some((_, lines)) = replacement
                .as_mut()
                .filter(|(range, _)| range.contains(&read))
            {
                if let Some(next) = lines.next() {
                    #[cfg(test)]
                    work::copy(std::mem::size_of::<Line>());
                    *line = next;
                    #[cfg(test)]
                    {
                        replaced = true;
                    }
                    false
                } else {
                    true
                }
            } else {
                false
            };
            if !remove {
                if read != write {
                    #[cfg(test)]
                    work::copy(std::mem::size_of::<Line>());
                }
                #[cfg(test)]
                {
                    moved += usize::from(replaced || read != write);
                }
                write += 1;
            }
            read += 1;
            !remove
        });
        for (_, cache) in caches {
            #[cfg(test)]
            {
                self.reference_cache_visits += 1;
            }
            debug_assert!(cache.lines.is_empty());
            cache.lines = write..write;
        }
        #[cfg(test)]
        {
            self.reference_moved_lines += moved;
        }
        self.stable_lines = self.stable_lines + self.lines.len() - previous_length;
        for index in self.stable_lines..previous_stable {
            self.visibility.set(index, false);
        }
        for index in first_line..self.stable_lines {
            self.index_visible(index);
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

#[cfg(test)]
thread_local! { pub(super) static COMPILE_PROVISIONAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
thread_local! { pub(super) static COPY_SUFFIX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
thread_local! { pub(crate) static RESCAN_BUDGET: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
