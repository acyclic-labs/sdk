use super::{Line, StreamText};
use crate::{
    client::{FileQueryView, OutputOwner},
    task_volume::{FileKey, HeadFile, Query},
};
use std::sync::Arc;

#[derive(Default)]
pub(super) struct Archive {
    source: Option<Arc<Query>>,
    first: Option<FileKey>,
    last: Option<(u64, FileKey)>,
    after: u64,
    stream: StreamText,
    rows: Vec<Line<'static>>,
    settled: usize,
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
    pub(super) fn rows(&self) -> &[Line<'static>] {
        &self.rows
    }
    #[cfg(test)]
    pub(super) fn work(&self) -> usize {
        self.stream.work
    }
    pub(super) fn update(&mut self, source: Option<(&FileQueryView, OutputOwner)>) -> usize {
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
            || self
                .first
                .as_ref()
                .is_some_and(|key| !files.files().contains_key(key))
        {
            *self = Self {
                source: Some(files.source().clone()),
                after,
                ..Self::default()
            };
            changed = 0;
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
        for (key, row) in initial.chain(tail) {
            dirty = true;
            self.last = Some((row.sequence, key.clone()));
            if row.sequence <= after {
                continue;
            }
            self.first.get_or_insert_with(|| key.clone());
            if let Some(text) = files.command_prompt(key) {
                changed = changed.min(self.stream.literal(prompt_rows(&text)));
            }
            let Some(text) = &row.text else {
                continue;
            };
            changed = changed.min(self.stream.boundary());
            changed = changed.min(self.stream.push(text.as_bytes()));
        }
        let same_pending =
            self.pending.len() == files.pending_prompts(&owner).count()
                && self
                    .pending
                    .iter()
                    .zip(files.pending_prompts(&owner))
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
                .pending_prompts(&owner)
                .filter_map(|key| {
                    Some((
                        files.files().get(key)?.clone(),
                        prompt_rows(&files.command_prompt(key)?),
                    ))
                })
                .collect();
            dirty = true;
        }
        if !dirty {
            return self.rows.len();
        }
        self.rows.truncate(changed);
        self.rows.extend(self.stream.rows(changed));
        self.settled = self.rows.len();
        for (_, rows) in &self.pending {
            self.rows.extend_from_slice(rows);
        }
        changed
    }
}
