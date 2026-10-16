//! Disposable folds over the shared volume snapshot and cursor follow.

use crate::{
    Error, Result,
    task_volume::{Change, Cursor, FileKey, FileValue, Query, SCHEMA, Snapshot},
};
use std::collections::BTreeMap;

/// Bounded, shared file slices from one accepted volume query.
#[derive(Clone, Debug, PartialEq)]
pub struct FileQueryView {
    query: Query,
    files: BTreeMap<FileKey, FileValue>,
    cursor: Cursor,
    bytes: usize,
    maximum_files: usize,
    maximum_bytes: usize,
}

impl FileQueryView {
    /// Replaces a disposable view at the snapshot's atomic cut.
    pub fn from_snapshot(
        query: Query,
        snapshot: Snapshot,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<Self> {
        let invalid = || Error::Conflict("file query snapshot requires reset".into());
        let digest = blake3::hash(
            &serde_json::to_vec(&query).map_err(|error| Error::Invalid(error.to_string()))?,
        );
        let cursor = snapshot.cursor;
        if maximum_files == 0
            || maximum_bytes == 0
            || maximum_files as u64 > crate::conversation::MAX_PORTABLE_COUNT as u64
            || maximum_bytes as u64 > crate::conversation::MAX_EXACT_JS_INTEGER
            || snapshot.files.len() > maximum_files
            || cursor.query != *digest.as_bytes()
            || cursor.schema != SCHEMA
            || cursor.position == 0
            || cursor.position > cursor.accepted
            || cursor.durable > cursor.accepted
            || cursor.volume != snapshot.generation.volume
            || cursor.epoch != snapshot.generation.epoch
            || cursor.position != snapshot.generation.position
            || cursor.commit != snapshot.generation.commit
        {
            return Err(invalid());
        }
        let mut files = BTreeMap::new();
        let mut bytes = 0usize;
        for file in snapshot.files {
            bytes = bytes
                .checked_add(file.bytes.len())
                .filter(|size| *size <= maximum_bytes)
                .ok_or_else(invalid)?;
            if !query.matches(&file.key) || files.insert(file.key.clone(), file).is_some() {
                return Err(invalid());
            }
        }
        Ok(Self {
            query,
            files,
            cursor,
            bytes,
            maximum_files,
            maximum_bytes,
        })
    }

    /// Exact resume cut; it advances even for nonmatching publications.
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    /// Current file slices, sharing their immutable query payloads.
    pub fn files(&self) -> &BTreeMap<FileKey, FileValue> {
        &self.files
    }

    /// Plain panel rows derived from the current private plan files.
    pub fn plan_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for (key, file) in &self.files {
            if !matches!(key, FileKey::Plan(_)) {
                continue;
            }
            lines.push(key.path());
            let markdown = String::from_utf8_lossy(&file.bytes);
            if let Some(list) = super::parse_task_list(&markdown) {
                for task in list.items() {
                    for (index, line) in task.text.lines().enumerate() {
                        lines.push(format!(
                            "{} {}",
                            if index == 0 {
                                task.state.marker()
                            } else {
                                "   "
                            },
                            safe(line)
                        ));
                    }
                }
            } else {
                lines.extend(markdown.lines().map(safe));
            }
        }
        lines
    }

    /// Folds the next accepted record atomically. Gaps require a new snapshot.
    pub fn apply(&mut self, change: Change) -> Result<bool> {
        let invalid = || Error::Conflict("file query follow requires reset".into());
        let cursor = &change.cursor;
        let files = change.file.iter().chain(&change.files);
        let changed_files = usize::from(change.file.is_some())
            .checked_add(change.files.len())
            .and_then(|count| count.checked_add(change.removed.len()))
            .ok_or_else(invalid)?;
        if changed_files > self.maximum_files.saturating_add(self.files.len()) {
            return Err(invalid());
        }
        let mut keys = std::collections::BTreeSet::new();
        for key in files.clone().map(|file| &file.key).chain(&change.removed) {
            if !self.query.matches(key) || !keys.insert(key) {
                return Err(invalid());
            }
        }
        if *cursor == self.cursor {
            return if files
                .clone()
                .all(|file| self.files.get(&file.key) == Some(file))
                && change
                    .removed
                    .iter()
                    .all(|key| !self.files.contains_key(key))
            {
                Ok(false)
            } else {
                Err(invalid())
            };
        }
        if cursor.volume != self.cursor.volume
            || cursor.query != self.cursor.query
            || cursor.epoch != self.cursor.epoch
            || cursor.schema != self.cursor.schema
            || self.cursor.position.checked_add(1) != Some(cursor.position)
            || cursor.accepted < self.cursor.accepted
            || cursor.accepted < cursor.position
            || cursor.durable < self.cursor.durable
            || cursor.durable > cursor.accepted
        {
            return Err(invalid());
        }
        let mut bytes = self.bytes;
        let mut count = self.files.len();
        for key in &change.removed {
            let old = self.files.get(key).ok_or_else(invalid)?;
            bytes = bytes.checked_sub(old.bytes.len()).ok_or_else(invalid)?;
            count -= 1;
        }
        let mut changed = !change.removed.is_empty();
        for file in files.clone() {
            let old = self.files.get(&file.key);
            bytes = bytes
                .checked_sub(old.map_or(0, |file| file.bytes.len()))
                .ok_or_else(invalid)?;
            count = count
                .checked_add(usize::from(old.is_none()))
                .ok_or_else(invalid)?;
            changed |= old != Some(file);
        }
        for file in files {
            bytes = bytes.checked_add(file.bytes.len()).ok_or_else(invalid)?;
        }
        if bytes > self.maximum_bytes || count > self.maximum_files {
            return Err(invalid());
        }
        for key in change.removed {
            self.files.remove(&key);
        }
        for file in change.file.into_iter().chain(change.files) {
            self.files.insert(file.key.clone(), file);
        }
        self.bytes = bytes;
        self.cursor = change.cursor;
        Ok(changed)
    }
}

fn safe(text: &str) -> String {
    let mut result = String::new();
    for character in text.chars() {
        if character.is_control() {
            result.extend(character.escape_debug());
        } else {
            result.push(character);
        }
    }
    result
}

impl super::ClientViews {
    /// Publishes a replacement file query after initial observation or reset.
    pub fn query_snapshot(
        &mut self,
        query: Query,
        snapshot: Snapshot,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<()> {
        let view = FileQueryView::from_snapshot(query, snapshot, maximum_files, maximum_bytes)?;
        self.queries
            .insert((view.cursor.volume, view.cursor.query), view);
        Ok(())
    }

    /// Updates the same shared views consumed by terminal, headless and browser renderers.
    pub fn query_change(&mut self, change: Change) -> Result<bool> {
        self.queries
            .get_mut(&(change.cursor.volume, change.cursor.query))
            .ok_or_else(|| Error::Conflict("file query snapshot is missing".into()))?
            .apply(change)
    }
}
