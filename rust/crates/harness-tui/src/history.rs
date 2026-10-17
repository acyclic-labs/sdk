use crate::buffer::{Buffer, normalize, slice};
use std::{collections::BTreeSet, ops::Range};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Selection {
    cursor: usize,
    anchor: usize,
}

impl Selection {
    fn of(buffer: &Buffer) -> Self {
        Self {
            cursor: buffer.cursor,
            anchor: buffer.anchor,
        }
    }
    fn restore(self, buffer: &mut Buffer) {
        buffer.cursor = self.cursor;
        buffer.anchor = self.anchor;
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Delta {
    start: usize,
    removed: String,
    inserted: String,
    pub before: Selection,
    pub after: Selection,
    removed_folds: Vec<Range<usize>>,
    added_folds: Vec<Range<usize>>,
}

impl Delta {
    pub fn capture(buffer: &Buffer, range: Range<usize>, text: &str) -> Self {
        Self {
            start: range.start,
            removed: slice(&buffer.text, range).to_owned(),
            inserted: text.to_owned(),
            before: Selection::of(buffer),
            after: Selection::of(buffer),
            removed_folds: buffer.folds.clone(),
            added_folds: vec![],
        }
    }

    pub fn finish(&mut self, buffer: &Buffer) {
        self.after = Selection::of(buffer);
        let after: BTreeSet<_> = buffer.folds.iter().map(|r| (r.start, r.end)).collect();
        let mut preserved = BTreeSet::new();
        self.removed_folds.retain(|range| {
            let mapped = shift(range, self.start, self.removed.len(), self.inserted.len());
            if let Some(mapped) = mapped.filter(|r| after.contains(&(r.start, r.end))) {
                preserved.insert((mapped.start, mapped.end));
                false
            } else {
                true
            }
        });
        self.removed_folds.shrink_to_fit();
        self.added_folds = buffer
            .folds
            .iter()
            .filter(|r| !preserved.contains(&(r.start, r.end)))
            .cloned()
            .collect();
    }

    pub fn between(before: &Buffer, after: &Buffer) -> Self {
        let prefix: usize = before
            .text
            .chars()
            .zip(after.text.chars())
            .take_while(|(a, b)| a == b)
            .map(|(c, _)| c.len_utf8())
            .sum();
        let suffix: usize = slice(&before.text, prefix..)
            .chars()
            .rev()
            .zip(slice(&after.text, prefix..).chars().rev())
            .take_while(|(a, b)| a == b)
            .map(|(c, _)| c.len_utf8())
            .sum();
        let mut delta = Self::capture(
            before,
            prefix..before.text.len() - suffix,
            slice(&after.text, prefix..after.text.len() - suffix),
        );
        delta.finish(after);
        delta
    }

    pub fn apply(&self, buffer: &mut Buffer, backwards: bool) {
        let (old, new, remove, restore, selection) = if backwards {
            (
                &self.inserted,
                &self.removed,
                &self.added_folds,
                &self.removed_folds,
                self.before,
            )
        } else {
            (
                &self.removed,
                &self.inserted,
                &self.removed_folds,
                &self.added_folds,
                self.after,
            )
        };
        let remove: BTreeSet<_> = remove.iter().map(|r| (r.start, r.end)).collect();
        buffer.folds = buffer
            .folds
            .iter()
            .filter(|r| !remove.contains(&(r.start, r.end)))
            .filter_map(|r| shift(r, self.start, old.len(), new.len()))
            .chain(restore.iter().cloned())
            .collect();
        buffer.folds.sort_by_key(|r| r.start);
        buffer.changed(self.start..self.start + old.len(), new.len());
        buffer
            .text
            .replace_range(self.start..self.start + old.len(), new);
        buffer.rebuild_boundaries();
        selection.restore(buffer);
    }

    fn heap_bytes(&self) -> usize {
        self.removed.capacity()
            + self.inserted.capacity()
            + (self.removed_folds.capacity() + self.added_folds.capacity())
                * size_of::<Range<usize>>()
    }
}

fn shift(range: &Range<usize>, start: usize, old: usize, new: usize) -> Option<Range<usize>> {
    if range.end <= start {
        Some(range.clone())
    } else if range.start >= start + old {
        Some(range.start - old + new..range.end - old + new)
    } else {
        None
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Edit {
    pub changes: Vec<Delta>,
    pub at: u64,
    pub kind: u8,
    pub size: usize,
    pub boundary: bool,
}

impl Edit {
    pub fn heap_bytes(&self) -> usize {
        self.changes.capacity() * size_of::<Delta>()
            + self.changes.iter().map(Delta::heap_bytes).sum::<usize>()
    }
}

/// Submitted prompts, with a saved in-progress draft and reverse search.
#[derive(Clone, Debug, Default)]
pub struct PromptHistory {
    entries: Vec<String>,
    generation: u64,
    pub(crate) navigation: Navigation,
}

// Search cancellation snapshots navigation without copying the shared entries.
#[derive(Clone, Debug, Default)]
pub(crate) struct Navigation {
    generation: u64,
    index: Option<usize>,
    draft: String,
    search_before: Option<usize>,
}

impl PromptHistory {
    /// Replace the history-file projection and invalidate saved navigation.
    pub fn replace(&mut self, snapshot: Self) {
        self.entries = snapshot.entries;
        self.generation = self.generation.wrapping_add(1);
        self.reset();
    }

    /// Revision of the loaded history projection, independent of its contents.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn restore_navigation(&mut self, saved: Navigation) {
        if saved.generation == self.generation {
            self.navigation = saved;
        } else {
            self.reset();
        }
    }

    /// Record a nonblank prompt, suppressing adjacent duplicates.
    pub fn push(&mut self, text: String) {
        // Only carriage returns need rewriting; keep the caller's allocation otherwise.
        let text = if text.contains('\r') {
            normalize(&text)
        } else {
            text
        };
        if !text.trim().is_empty() && self.entries.last() != Some(&text) {
            self.entries.push(text);
        }
        self.reset();
    }

    /// Return the submitted prompts in chronological order.
    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    /// Begin a new history navigation session.
    pub fn reset(&mut self) {
        self.navigation = Navigation {
            generation: self.generation,
            ..Navigation::default()
        };
    }

    /// Go backwards, saving the live draft on the first invocation.
    pub fn previous(&mut self, draft: &str) -> Option<String> {
        if self.entries.is_empty() {
            return None;
        }
        if self.navigation.index.is_none() {
            self.navigation.draft = normalize(draft);
        }
        let index = self
            .navigation
            .index
            .unwrap_or(self.entries.len())
            .saturating_sub(1);
        self.navigation.index = Some(index);
        self.entries.get(index).cloned()
    }

    /// Go forwards, restoring the draft after the newest entry.
    pub fn next_entry(&mut self) -> Option<String> {
        let index = self.navigation.index? + 1;
        if index >= self.entries.len() {
            self.navigation.index = None;
            Some(self.navigation.draft.clone())
        } else {
            self.navigation.index = Some(index);
            self.entries.get(index).cloned()
        }
    }

    /// Find the previous substring match; reset navigation to restart a query.
    pub fn reverse_search(&mut self, query: &str) -> Option<String> {
        let end = self.navigation.search_before.unwrap_or(self.entries.len());
        let found = self
            .entries
            .iter()
            .take(end)
            .enumerate()
            .rev()
            .find(|(_, entry)| entry.contains(query));
        if let Some((index, entry)) = found {
            self.navigation.search_before = Some(index);
            Some(entry.clone())
        } else {
            None
        }
    }
}
