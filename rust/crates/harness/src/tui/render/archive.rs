use super::{Line, StreamText};
use crate::{
    client::{FileQueryView, OutputOwner},
    task_volume::{FileKey, HeadFile, Query},
};
use std::{collections::VecDeque, sync::Arc};

#[derive(Default)]
pub(super) struct Archive {
    origins: super::source_rows::SourceRows,
    source: Option<Arc<Query>>,
    first: Option<FileKey>,
    last: Option<(u64, FileKey)>,
    after: u64,
    stream: StreamText,
    rows: Vec<Line<'static>>,
    settled: usize,
    blocks: VecDeque<(FileKey, usize)>,
    dropped: usize,
    pending: Vec<(HeadFile, Vec<Line<'static>>)>,
}

#[cfg(test)]
#[path = "archive_tests.rs"]
mod tests;
/// One row per prompt line. Lines are split before sanitizing: the sanitizer
/// escapes `\n` itself, so splitting its output would never break a line.
fn prompt_rows(text: &str) -> Vec<Line<'static>> {
    text.split_inclusive('\n')
        .chain((text.is_empty() || text.ends_with('\n')).then_some(""))
        .enumerate()
        .map(|(index, row)| {
            // Consume only complete delimiters; a standalone CR is content.
            let row = row
                .strip_suffix("\r\n")
                .or_else(|| row.strip_suffix('\n'))
                .unwrap_or(row);
            Line::raw(format!(
                "{}{}",
                if index == 0 { "You: " } else { "     " },
                super::super::display_text(row)
            ))
        })
        .collect()
}
impl Archive {
    pub(super) fn new(after: u64) -> Self {
        Self {
            after,
            ..Self::default()
        }
    }
    pub(super) fn after(&self) -> u64 {
        self.after
    }
    pub(super) fn dropped(&self) -> usize {
        self.dropped
    }
    pub(super) fn rows(&self) -> &[Line<'static>] {
        &self.rows
    }
    pub(super) fn origin(&self, line: usize) -> Option<crate::tui::TranscriptOrigin> {
        self.origins.origin(line)
    }
    pub(super) fn resolve(&self, origin: crate::tui::TranscriptOrigin) -> Option<usize> {
        self.origins.resolve(origin)
    }
    #[cfg(test)]
    pub(super) fn work(&self) -> usize {
        self.stream.work
    }
    fn discard_prefix(&mut self, files: &FileQueryView) -> bool {
        if self
            .first
            .as_ref()
            .is_some_and(|key| !files.files().contains_key(key))
        {
            let mut through = 0;
            while self
                .blocks
                .front()
                .is_some_and(|(key, _)| !files.files().contains_key(key))
            {
                if let Some((_, end)) = self.blocks.pop_front() {
                    through = end;
                }
            }
            self.dropped = self.stream.discard_rows(through);
            self.origins.discard(self.dropped);
            self.rows.drain(..self.dropped.min(self.rows.len()));
            self.settled = self.settled.saturating_sub(self.dropped);
            for (_, end) in &mut self.blocks {
                *end = end.saturating_sub(self.dropped);
            }
            self.first = self.blocks.front().map(|(key, _)| key.clone());
            return true;
        }
        false
    }
    fn refresh_pending(&mut self, files: &FileQueryView, owner: &OutputOwner) -> bool {
        let same_pending =
            self.pending.len() == files.pending_prompts(owner).count()
                && self
                    .pending
                    .iter()
                    .zip(files.pending_prompts(owner))
                    .all(|((old, _), key)| {
                        old.key == *key
                            && files.files().get(key).is_some_and(|new| {
                                old.segments.len() == new.segments.len()
                                    && old.segments.iter().zip(&new.segments).all(|(a, b)| {
                                        a.as_ptr() == b.as_ptr() && a.len() == b.len()
                                    })
                            })
                    });
        if !same_pending {
            self.pending = files
                .pending_prompts(owner)
                .filter_map(|key| {
                    Some((
                        files.files().get(key)?.clone(),
                        prompt_rows(&files.command_prompt(key)?),
                    ))
                })
                .collect();
            return true;
        }
        false
    }
    pub(super) fn update(&mut self, source: Option<(&FileQueryView, OutputOwner)>) -> usize {
        self.dropped = 0;
        let Some((files, owner)) = source else {
            if self.source.is_some() {
                *self = Self::new(self.after);
                return 0;
            }
            return self.rows.len();
        };
        let after = self.after;
        let mut changed = self.settled;
        let mut dirty = false;
        if !self
            .source
            .as_ref()
            .is_some_and(|source| Arc::ptr_eq(source, files.source()))
        {
            *self = Self {
                source: Some(files.source().clone()),
                after,
                ..Self::default()
            };
            changed = 0;
            dirty = true;
        }
        if self.discard_prefix(files) {
            changed = self.settled;
            dirty = true;
        }
        let last = self.last.clone();
        let initial = last
            .is_none()
            .then(|| files.outputs(&owner, after))
            .into_iter()
            .flatten();
        let tail = last
            .as_ref()
            .map(|(seq, key)| files.outputs_from(&owner, Some((*seq, key))))
            .into_iter()
            .flatten();
        let mut pending_trimmed = false;
        for (key, row) in initial.chain(tail) {
            if !pending_trimmed {
                self.origins.truncate(self.settled);
                pending_trimmed = true;
            }
            dirty = true;
            self.last = Some((row.sequence, key.clone()));
            if row.sequence <= after {
                continue;
            }
            self.first.get_or_insert_with(|| key.clone());
            let previous = self.stream.len();
            if let Some(text) = files.command_prompt(key) {
                changed = changed.min(self.stream.literal(prompt_rows(&text)));
            }
            if let Some(text) = &row.text {
                changed = changed.min(self.stream.boundary());
                changed = changed.min(self.stream.push(text.as_bytes()));
            }
            // Each outcome is an independent Markdown entry. Sealing it here
            // makes its rendered prefix disposable without replaying successors.
            changed = changed.min(self.stream.boundary());
            self.blocks.push_back((key.clone(), self.stream.len()));
            if previous != 0 && self.stream.len() > previous {
                self.origins.extend_current(previous + 1);
            }
            self.origins
                .extend(super::source_rows::identity(key), self.stream.len());
        }
        dirty |= self.refresh_pending(files, &owner);
        if !dirty {
            return self.rows.len();
        }
        if !pending_trimmed {
            self.origins.truncate(self.settled);
        }
        self.rows.truncate(changed);
        self.rows.extend(self.stream.rows(changed));
        self.settled = self.rows.len();
        for (file, rows) in &self.pending {
            self.rows.extend_from_slice(rows);
            self.origins
                .extend(super::source_rows::identity(&file.key), self.rows.len());
        }
        changed
    }
}
