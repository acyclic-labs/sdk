//! Disposable file views at a query cursor. Bytes retain segment ownership.

use crate::task_volume::{
    Change, Channel, Cursor, Error, FileKey, HeadFile, HeadSnapshot, Result, TaskFile,
};
use std::collections::btree_map::Entry;
#[cfg(feature = "test-support")]
type Files = crate::test_support::TaskVolumeFiles<HeadFile>;
#[cfg(not(feature = "test-support"))]
type Files = std::collections::BTreeMap<FileKey, HeadFile>;

/// Bounds for one selected file query, including every retained segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct QueryViewLimits {
    /// Maximum materialized logical files.
    pub files: usize,
    /// Maximum retained immutable segments.
    pub segments: usize,
    /// Maximum retained logical payload bytes.
    pub bytes: u64,
}

/// One query's materialized files; snapshots and deltas never concatenate text.
#[derive(Clone, Debug, PartialEq)]
pub struct FileViews {
    pub(super) cursor: Cursor,
    pub(super) files: Files,
    stream_order: Vec<FileKey>,
    limits: QueryViewLimits,
    segments: usize,
    pub(super) bytes: u64,
    pub(super) window: bool,
}

fn admit(limits: QueryViewLimits, files: usize, segments: usize, bytes: u64) -> Result<()> {
    if files > limits.files || segments > limits.segments || bytes > limits.bytes {
        return Err(Error::LimitExceeded);
    }
    Ok(())
}

fn disposable(key: &FileKey) -> bool {
    matches!(key, FileKey::Task { file, .. }
        if !matches!(file, TaskFile::ExecutionResult(_) | TaskFile::ExecutionClaim(_)))
}

impl FileViews {
    /// Builds a complete replacement without mutating an existing fold.
    pub fn snapshot(snapshot: HeadSnapshot, limits: QueryViewLimits) -> Result<Self> {
        Self::build(snapshot, limits, true)
    }
    pub(super) fn ordinary(snapshot: HeadSnapshot, limits: QueryViewLimits) -> Result<Self> {
        Self::build(snapshot, limits, false)
    }
    fn build(snapshot: HeadSnapshot, limits: QueryViewLimits, materialized: bool) -> Result<Self> {
        if snapshot.cursor.schema != crate::task_volume::SCHEMA
            || snapshot.cursor.position == 0
            || snapshot.cursor.volume != snapshot.generation.volume
            || snapshot.cursor.epoch != snapshot.generation.epoch
            || snapshot.cursor.position != snapshot.generation.position
            || snapshot.cursor.commit != snapshot.generation.commit
            || snapshot.cursor.durable > snapshot.cursor.accepted
            || snapshot.cursor.position > snapshot.cursor.accepted
        {
            return Err(Error::ResetRequired);
        }
        let mut view = Self {
            cursor: snapshot.cursor,
            files: Files::default(),
            stream_order: Vec::new(),
            limits,
            segments: 0,
            bytes: 0,
            window: false,
        };
        for file in snapshot.files {
            let text = matches!(
                file.key,
                FileKey::Task {
                    file: TaskFile::TextHead { .. },
                    ..
                }
            );
            if materialized
                && (matches!(
                    file.key,
                    FileKey::Task {
                        file: TaskFile::Chunk { .. },
                        ..
                    }
                ) || text != file.through_sequence.is_some())
                || file.segments.is_empty()
            {
                return Err(Error::InvalidSegment);
            }
            let length = file.segments.iter().try_fold(0u64, |total, segment| {
                total
                    .checked_add(segment.len() as u64)
                    .ok_or(Error::LimitExceeded)
            })?;
            if length != file.length {
                return Err(Error::InvalidSegment);
            }
            let segments = view
                .segments
                .checked_add(file.segments.len())
                .ok_or(Error::LimitExceeded)?;
            let bytes = view.bytes.checked_add(length).ok_or(Error::LimitExceeded)?;
            admit(
                limits,
                view.files
                    .len()
                    .checked_add(1)
                    .ok_or(Error::LimitExceeded)?,
                segments,
                bytes,
            )?;
            if stream_key(&file.key) {
                view.stream_order.push(file.key.clone());
            }
            if view.files.insert(file.key.clone(), file).is_some() {
                return Err(Error::InvalidSegment);
            }
            view.segments = segments;
            view.bytes = bytes;
        }
        Ok(view)
    }

    /// Last applied query cut.
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    pub(crate) fn watermark(&mut self, value: acyclic_stream::StreamWatermarks) -> Result<()> {
        let mut cursor = self.cursor.clone();
        cursor.advance_watermarks(value)?;
        self.cursor = cursor;
        Ok(())
    }
    /// One materialized file.
    pub fn file(&self, key: &FileKey) -> Option<&HeadFile> {
        self.files.get(key)
    }
    /// Materialized streams preserve authoritative snapshot and follow order.
    pub fn files(&self) -> impl Iterator<Item = &HeadFile> {
        self.files
            .values()
            .filter(|head| !stream_key(&head.key))
            .chain(
                self.stream_order
                    .iter()
                    .filter_map(|key| self.files.get(key)),
            )
    }

    pub(super) fn replace(
        &mut self,
        change: &Change,
        rejected: &std::collections::BTreeSet<FileKey>,
    ) {
        for key in change
            .removed
            .iter()
            .chain(change.file.iter().chain(&change.files).map(|f| &f.key))
        {
            if let Some(old) = self.files.remove(key) {
                if stream_key(key) {
                    self.stream_order.retain(|old| old != key);
                }
                self.bytes -= old.length;
                self.segments -= old.segments.len();
            }
        }
        for file in change.file.iter().chain(&change.files) {
            if rejected.contains(&file.key) {
                continue;
            }
            self.bytes += file.bytes.len() as u64;
            self.segments += 1;
            if stream_key(&file.key) {
                self.stream_order.push(file.key.clone());
            }
            self.files.insert(
                file.key.clone(),
                HeadFile {
                    key: file.key.clone(),
                    publication: file.publication,
                    channel: file.channel,
                    segments: vec![file.bytes.clone()],
                    length: file.bytes.len() as u64,
                    through_sequence: None,
                },
            );
        }
        self.cursor = change.cursor.clone();
    }

    /// Prepares one change without altering or borrowing the fold across I/O.
    /// Read-only renderers remain usable while publication is pending. Applying
    /// the change checks the original cut before committing the prepared totals.
    pub fn prepare<'a>(
        &self,
        change: &'a Change,
    ) -> Result<impl FnOnce(&mut Self) -> Result<()> + use<'a>> {
        // TaskVolume appends one immutable event. A different source's atomic
        // file replacements/removals must not be silently reduced as that event.
        if !change.files.is_empty() || !change.removed.is_empty() {
            return Err(Error::InvalidSegment);
        }
        let next = &change.cursor;
        if next.volume != self.cursor.volume
            || next.epoch != self.cursor.epoch
            || next.schema != self.cursor.schema
            || next.query != self.cursor.query
            || self.cursor.position.checked_add(1) != Some(next.position)
            || next.accepted < self.cursor.accepted.max(next.position)
            || next.durable < self.cursor.durable
            || next.durable > next.accepted
        {
            return Err(Error::ResetRequired);
        }
        let logical = change
            .file
            .as_ref()
            .map(|file| file.head_key())
            .transpose()?;
        let files = &self.files;
        let other_sequence = match logical.as_ref() {
            Some((
                FileKey::Task {
                    task,
                    file:
                        TaskFile::TextHead {
                            turn,
                            execution,
                            channel,
                        },
                },
                _,
            )) => {
                let channel = match channel {
                    Channel::Text => Channel::Reasoning,
                    Channel::Reasoning => Channel::Text,
                };
                files
                    .get(&FileKey::Task {
                        task: task.clone(),
                        file: TaskFile::TextHead {
                            execution: *execution,
                            turn: *turn,
                            channel,
                        },
                    })
                    .and_then(|head| head.through_sequence)
            }
            _ => None,
        };
        let count = files.len();
        let before = logical.as_ref().and_then(|(key, _)| files.get(key));
        let mut bytes = self.bytes;
        let mut segments = self.segments;
        let mut next_count = count;
        let mut length = 0;
        if let Some(file) = change.file.as_ref() {
            let sequence = logical.as_ref().and_then(|(_, sequence)| *sequence);
            if let Some(sequence) = sequence {
                let last = before
                    .and_then(|head| head.through_sequence)
                    .into_iter()
                    .chain(other_sequence)
                    .max();
                if last.is_some_and(|last| sequence <= last) {
                    return Err(Error::SequenceConflict(file.key.clone()));
                }
            } else if let Some(before) = before {
                if !file.key.mutable()
                    && (before.segments.len() != 1
                        || before.segments[0] != file.bytes
                        || before.publication != file.publication)
                {
                    return Err(Error::ImmutableConflict(file.key.clone()));
                }
            }
            let replaced = before.filter(|_| sequence.is_none());
            bytes = bytes
                .checked_sub(replaced.map_or(0, |head| head.length))
                .and_then(|total| total.checked_add(file.bytes.len() as u64))
                .ok_or(Error::LimitExceeded)?;
            segments = segments
                .checked_sub(replaced.map_or(0, |head| head.segments.len()))
                .and_then(|total| total.checked_add(1))
                .ok_or(Error::LimitExceeded)?;
            length = before
                .filter(|_| sequence.is_some())
                .map_or(0, |head| head.length)
                .checked_add(file.bytes.len() as u64)
                .ok_or(Error::LimitExceeded)?;
            next_count = count
                .checked_add(usize::from(before.is_none()))
                .ok_or(Error::LimitExceeded)?;
        }
        // Session observations publish each raw segment once, then retain only
        // a bounded display tail. Session request/claim/result files are never
        // candidates for eviction. Validate sequence ownership before trimming.
        let mut evictions = Vec::new();
        if self.window {
            let incoming = logical.as_ref().map(|(key, _)| key);
            let mut task_segments = self
                .files
                .values()
                .filter(|head| disposable(&head.key))
                .map(|head| head.segments.len())
                .sum::<usize>();
            let mut task_bytes = self
                .files
                .values()
                .filter(|head| disposable(&head.key))
                .map(|head| head.length)
                .sum::<u64>();
            if let Some(file) = change.file.as_ref().filter(|file| disposable(&file.key)) {
                let replaced = before.filter(|_| logical.as_ref().unwrap().1.is_none());
                task_segments = task_segments - replaced.map_or(0, |head| head.segments.len()) + 1;
                task_bytes =
                    task_bytes - replaced.map_or(0, |head| head.length) + file.bytes.len() as u64;
            }
            for head in self.files.values().filter(|head| disposable(&head.key)) {
                if Some(&head.key) == incoming && logical.as_ref().unwrap().1.is_none() {
                    continue;
                }
                let mut removed = 0;
                for segment in &head.segments {
                    if task_segments <= 128 && task_bytes <= self.limits.bytes / 2 {
                        break;
                    }
                    task_segments -= 1;
                    task_bytes -= segment.len() as u64;
                    segments -= 1;
                    bytes -= segment.len() as u64;
                    removed += 1;
                }
                if removed > 0 {
                    if removed == head.segments.len() && Some(&head.key) != incoming {
                        next_count -= 1;
                    }
                    evictions.push((head.key.clone(), removed));
                }
            }
            if task_segments > 128 || task_bytes > self.limits.bytes / 2 {
                return Err(Error::LimitExceeded);
            }
        }
        admit(self.limits, next_count, segments, bytes)?;
        let expected = self.cursor.clone();
        Ok(move |view: &mut Self| {
            if view.cursor != expected {
                return Err(Error::ResetRequired);
            }
            admit(view.limits, next_count, segments, bytes)?;
            if let (Some(file), Some((key, sequence))) = (&change.file, logical) {
                match view.files.entry(key.clone()) {
                    Entry::Occupied(mut entry) if sequence.is_some() => {
                        let head = entry.get_mut();
                        head.segments.push(file.bytes.clone());
                        head.length = length;
                        head.through_sequence = sequence;
                    }
                    Entry::Occupied(mut entry) => {
                        let head = entry.get_mut();
                        if head.segments.len() != 1 || head.segments[0] != file.bytes {
                            head.segments = vec![file.bytes.clone()];
                        }
                        head.length = length;
                        head.publication = file.publication;
                        head.channel = file.channel;
                    }
                    Entry::Vacant(entry) => {
                        if stream_key(&key) {
                            view.stream_order.push(key.clone());
                        }
                        entry.insert(HeadFile {
                            key,
                            publication: file.publication,
                            channel: file.channel,
                            segments: vec![file.bytes.clone()],
                            length,
                            through_sequence: sequence,
                        });
                    }
                }
            }
            for (key, count) in evictions {
                let Entry::Occupied(mut entry) = view.files.entry(key) else {
                    unreachable!("prepared retained head")
                };
                let head = entry.get_mut();
                let removed = head
                    .segments
                    .drain(..count)
                    .map(|bytes| bytes.len() as u64)
                    .sum::<u64>();
                head.length -= removed;
                if head.segments.is_empty() {
                    view.stream_order.retain(|key| key != entry.key());
                    entry.remove();
                }
            }
            view.bytes = bytes;
            view.segments = segments;
            view.cursor = next.clone();
            Ok(())
        })
    }
}

fn stream_key(key: &FileKey) -> bool {
    matches!(
        key,
        FileKey::Task {
            file: TaskFile::Chunk { .. } | TaskFile::TextHead { .. },
            ..
        }
    )
}
