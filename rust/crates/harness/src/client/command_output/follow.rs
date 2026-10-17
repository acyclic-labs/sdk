//! Disposable display selection. Execution inbox files stay in CommandSession.
use super::Metadata;
use crate::task_volume::{Change, FileKey, FileValue, Observation, TaskFile};
use crate::task_volume::{Error, Result};
use futures::StreamExt as _;
use std::collections::{BTreeMap, BTreeSet};

/// The session's encoded-file admission limit, independent of retained display.
#[derive(Clone, Copy)]
pub(in crate::client) struct PerFileByteLimit(pub u64);

/// Total retained answer selection, supplied by the renderer rather than the writer.
#[derive(Clone, Copy)]
pub(in crate::client) struct DisplayWindowBudget {
    pub files: usize,
    pub bytes: usize,
}

struct Window {
    files: BTreeMap<FileKey, (usize, u64)>,
    order: BTreeSet<(u64, FileKey)>,
    bytes: usize,
    per_file: PerFileByteLimit,
    total: DisplayWindowBudget,
    recent: usize,
}
impl Window {
    fn remove(&mut self, key: &FileKey) {
        if let Some((bytes, sequence)) = self.files.remove(key) {
            self.bytes -= bytes;
            self.order.remove(&(sequence, key.clone()));
        }
    }
    fn include(&mut self, file: &FileValue) -> Result<bool> {
        let sequence = match &file.key {
            FileKey::Task {
                file: TaskFile::ExecutionResult(_),
                ..
            }
            | FileKey::Session {
                file: TaskFile::ExecutionResult(_),
                ..
            } => {
                let Some(metadata) = Metadata::from_file(file) else {
                    return Ok(false);
                };
                metadata.sequence
            }
            // Drafts, history, catalogs and UI state have their own live queries.
            // They can neither consume answer capacity nor be evicted as answers.
            _ => return Ok(false),
        };
        if file.bytes.len() as u64 > self.per_file.0 {
            return Err(Error::LimitExceeded);
        }
        self.remove(&file.key);
        self.bytes = self
            .bytes
            .checked_add(file.bytes.len())
            .ok_or(Error::LimitExceeded)?;
        self.files
            .insert(file.key.clone(), (file.bytes.len(), sequence));
        self.order.insert((sequence, file.key.clone()));
        Ok(true)
    }
    fn trim<const TRIM: bool>(&mut self, removed: &mut Vec<FileKey>) -> Result<()> {
        if !TRIM {
            return Ok(());
        }
        while self.order.len() > self.recent
            || self.files.len() > self.total.files
            || self.bytes > self.total.bytes
        {
            let key = self
                .order
                .first()
                .map(|(_, key)| key.clone())
                .ok_or(Error::LimitExceeded)?;
            self.remove(&key);
            removed.push(key);
        }
        Ok(())
    }
    fn apply<const TRIM: bool>(&mut self, change: &mut Change) -> Result<()> {
        change.removed.retain(|key| {
            let retained = self.files.contains_key(key);
            self.remove(key);
            retained
        });
        let mut added = BTreeSet::new();
        if let Some(file) = &change.file {
            if !self.files.contains_key(&file.key) {
                added.insert(file.key.clone());
            }
            if !self.include(file)? {
                change.file = None;
            }
        }
        let mut files = Vec::with_capacity(change.files.len());
        for file in change.files.drain(..) {
            if !self.files.contains_key(&file.key) {
                added.insert(file.key.clone());
            }
            if self.include(&file)? {
                files.push(file);
            }
        }
        let mut retired = Vec::new();
        self.trim::<TRIM>(&mut retired)?;
        change
            .removed
            .extend(retired.into_iter().filter(|key| !added.contains(key)));
        files.retain(|file| self.files.contains_key(&file.key));
        change.files = files;
        if change
            .file
            .as_ref()
            .is_some_and(|file| !self.files.contains_key(&file.key))
        {
            change.file = None;
        }
        Ok(())
    }
}

pub(in crate::client) fn observe(
    mut observation: Observation,
    recent: usize,
    per_file: PerFileByteLimit,
    total: DisplayWindowBudget,
) -> Result<Observation> {
    let snapshot = observation.snapshot.as_mut().ok_or(Error::ResetRequired)?;
    let mut window = Window {
        files: BTreeMap::new(),
        order: BTreeSet::new(),
        bytes: 0,
        recent,
        per_file,
        total,
    };
    let mut files = Vec::new();
    for file in snapshot.files.drain(..) {
        if window.include(&file)? {
            files.push(file);
        }
    }
    window.trim::<true>(&mut Vec::new())?;
    files.retain(|file| window.files.contains_key(&file.key));
    snapshot.files = files;
    observation.changes = Box::pin(futures::stream::try_unfold(
        (observation.changes, window),
        |(mut changes, mut window)| async move {
            let Some(change) = changes.next().await else {
                return Ok(None);
            };
            let mut change = change?;
            window.apply::<true>(&mut change)?;
            Ok(Some((change, (changes, window))))
        },
    ));
    Ok(observation)
}

#[cfg(test)]
mod tests;
