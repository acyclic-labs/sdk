use super::*;

impl Markdown {
    pub(super) fn resolve(&mut self, keys: &HashSet<Key>) -> usize {
        let mut affected: HashSet<usize> = HashSet::new();
        for key in keys {
            if let Some(indices) = self.dependencies.get(key) {
                affected.extend(indices);
            }
        }
        if affected.is_empty() {
            return self.stable_lines;
        }
        let mut changed = self.stable_lines;
        let mut replacements = HashMap::new();
        let mut length = self.lines.len();
        let mut resize = false;
        for index in affected {
            #[cfg(test)]
            {
                self.reference_cache_visits += 1;
            }
            let Some(cache) = self.cached.get(index) else {
                continue;
            };
            let mut renderer = cache.state.checkpoint();
            for event in cache.template.events(&self.references) {
                renderer.event_in(&event, Some(&self.references));
            }
            renderer.flush();
            changed = changed.min(cache.lines.start);
            length = length - cache.lines.len() + renderer.lines.len();
            resize |= cache.lines.len() != renderer.lines.len();
            replacements.insert(index, renderer.lines);
        }
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
        changed
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
        self.stable_lines = self.stable_lines - previous_length + lines.len();
        self.lines = lines;
        for index in self.stable_lines..previous_stable {
            self.visibility.set(index, false);
        }
        for index in first_line..self.stable_lines {
            self.index_visible(index);
        }
    }
}
