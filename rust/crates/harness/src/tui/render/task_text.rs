use super::{Line, StreamText};
use crate::{
    OperationId,
    client::TaskQueryView,
    task_volume::{Channel, Component, FileKey, Query, TaskFile},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::{Bound, Range},
    sync::Arc,
};

struct Entry {
    position: (u64, usize),
    decoder: StreamText,
    rows: Range<usize>,
    separator: bool,
}

struct Patch {
    range: Range<usize>,
    rows: Vec<Line<'static>>,
}

/// Disposable per-stream checkpoints over the followed task files. Text and
/// reasoning own separate instances; executions and turns own separate decoders.
#[derive(Default)]
pub(super) struct TaskText {
    identity: Option<(Arc<Query>, Component, Channel)>,
    first: Option<FileKey>,
    pub(super) cut: u64,
    entries: BTreeMap<(OperationId, u64), Entry>,
    order: BTreeMap<(u64, usize), (OperationId, u64)>,
    visible: BTreeSet<(u64, usize)>,
    open: BTreeSet<(u64, OperationId)>,
    rows: Vec<Line<'static>>,
    patches: Vec<Patch>,
    #[cfg(test)]
    pub(super) work: usize,
    #[cfg(test)]
    pub(super) row_work: usize,
}

impl TaskText {
    pub(super) fn rows(&self) -> &[Line<'static>] {
        &self.rows
    }

    /// Apply only changed row ranges. Unchanged rows retain their owned strings,
    /// including the rows after an earlier execution's extended head.
    pub(super) fn apply(&self, rows: &mut Vec<Line<'static>>, offset: usize) {
        for patch in &self.patches {
            rows.splice(
                offset + patch.range.start..offset + patch.range.end,
                patch.rows.iter().inspect(|_| examined()).cloned(),
            );
        }
    }

    pub(super) fn update(&mut self, task: &TaskQueryView<'_>, channel: Channel) -> usize {
        self.patches.clear();
        let mut changed = self.rows.len();
        if !self
            .identity
            .as_ref()
            .is_some_and(|(source, owner, bound_channel)| {
                Arc::ptr_eq(source, task.source) && owner == task.task && *bound_channel == channel
            })
            || self
                .first
                .as_ref()
                .is_some_and(|key| task.file(key).is_none())
        {
            self.entries.clear();
            self.order.clear();
            self.visible.clear();
            self.open.clear();
            self.first = None;
            self.cut = 0;
            self.identity = Some((task.source.clone(), task.task.clone(), channel));
            self.patches.push(Patch {
                range: 0..self.rows.len(),
                rows: Vec::new(),
            });
            self.rows.clear();
            changed = 0;
        }
        // The latest-update index visits each changed head once, even across
        // skipped render frames. First publication orders newly observed scopes.
        let mut updates: Vec<_> = task
            .text_updates(self.cut)
            .filter(|(_, text)| text.channel == channel)
            .collect();
        if updates.iter().any(|(position, text)| {
            examined();
            self.entries
                .get(&(text.execution, text.turn))
                .is_some_and(|entry| {
                    matches!(
                        text.file.key,
                        FileKey::Task {
                            file: TaskFile::TextHead { .. },
                            ..
                        }
                    ) && entry.position != *position
                })
        }) {
            // Ordinary mutable-file replacement can move a head's publication
            // position. Append-only materialized heads retain that position.
            self.identity = None;
            return self.update(task, channel);
        }
        updates.sort_unstable_by_key(|(position, _)| *position);
        for (position, text) in updates {
            let identity = (text.execution, text.turn);
            if !self.entries.contains_key(&identity) {
                let start = self
                    .order
                    .range(..position)
                    .next_back()
                    .map_or(0, |(_, identity)| self.entries[identity].rows.end);
                self.order.insert(position, identity);
                self.entries.insert(
                    identity,
                    Entry {
                        position,
                        decoder: StreamText::default(),
                        rows: start..start,
                        separator: false,
                    },
                );
            }
            let entry = self.entries.get_mut(&identity).expect("inserted stream");
            let decoder = &mut entry.decoder;
            let reset = decoder.prefix.as_ref().is_some_and(|(key, index, bytes)| {
                task.file(key)
                    .and_then(|file| file.segments.get(*index))
                    .is_none_or(|current| {
                        current.as_ptr() != bytes.as_ptr() || current.len() != bytes.len()
                    })
            });
            if reset {
                *decoder = StreamText::default();
            }
            let start = if decoder.last.as_ref() == Some(&text.file.key) {
                decoder.segments
            } else {
                0
            };
            #[cfg(test)]
            let before = decoder.work;
            let mut local = if reset {
                0
            } else {
                decoder
                    .provisional_row
                    .take()
                    .unwrap_or_else(|| decoder.len())
            };
            for (index, bytes) in text.file.segments.iter().enumerate().skip(start) {
                if decoder.prefix.is_none() && !bytes.is_empty() {
                    decoder.prefix = Some((text.file.key.clone(), index, bytes.clone()));
                }
                local = local.min(decoder.push(bytes));
            }
            decoder.last = Some(text.file.key.clone());
            decoder.segments = text.file.segments.len();
            self.open.insert((text.turn, text.execution));
            self.first.get_or_insert_with(|| text.file.key.clone());
            #[cfg(test)]
            {
                self.work += decoder.work - before;
            }
            changed = changed.min(self.patch(identity, local));
        }
        // A completion can arrive without another text publication.
        let completed: Vec<_> = self
            .open
            .iter()
            .filter_map(|(turn, execution)| {
                examined();
                (task
                    .file(&FileKey::Task {
                        task: task.task.clone(),
                        file: TaskFile::Completion(*turn),
                    })
                    .is_some())
                .then_some((*execution, *turn))
            })
            .collect();
        for identity in completed {
            let entry = self.entries.get_mut(&identity).expect("completed stream");
            #[cfg(test)]
            let before = entry.decoder.work;
            let local = entry.decoder.boundary();
            #[cfg(test)]
            {
                self.work += entry.decoder.work - before;
            }
            self.open.remove(&(identity.1, identity.0));
            changed = changed.min(self.patch(identity, local));
        }
        self.cut = task.cursor.position;
        changed
    }

    fn patch(&mut self, identity: (OperationId, u64), local: usize) -> usize {
        let entry = self.entries.get_mut(&identity).expect("changed stream");
        let position = entry.position;
        let visible = entry.decoder.len() != 0 || entry.decoder.pending_len != 0;
        let separator = visible && self.visible.range(..position).next_back().is_some();
        let replaced_separator = entry.separator != separator;
        let local = if replaced_separator { 0 } else { local };
        let prefix = if replaced_separator {
            0
        } else {
            usize::from(entry.separator)
        };
        let begin = entry.rows.start + local.min(entry.rows.len() - prefix) + prefix;
        let range = begin..entry.rows.end;
        let mut rows = entry.decoder.rows(local);
        if replaced_separator && separator {
            rows.insert(0, Line::default());
        }
        entry.separator = separator;
        if visible {
            self.visible.insert(position);
        } else {
            self.visible.remove(&position);
        }
        let mut changed = self.replace(identity, range, rows);
        // Visibility changes can alter just the next visible scope's separator.
        if let Some(next) = self
            .visible
            .range((Bound::Excluded(position), Bound::Unbounded))
            .next()
            .copied()
        {
            let identity = self.order[&next];
            let entry = self.entries.get_mut(&identity).expect("visible stream");
            let separator = self.visible.range(..next).next_back().is_some();
            if separator != entry.separator {
                let range = entry.rows.start..entry.rows.start + usize::from(entry.separator);
                let rows = if separator {
                    vec![Line::default()]
                } else {
                    Vec::new()
                };
                entry.separator = separator;
                changed = changed.min(self.replace(identity, range, rows));
            }
        }
        changed
    }

    fn replace(
        &mut self,
        identity: (OperationId, u64),
        range: Range<usize>,
        rows: Vec<Line<'static>>,
    ) -> usize {
        #[cfg(test)]
        {
            self.row_work += rows.len();
        }
        let old = range.len();
        let new = rows.len();
        self.rows
            .splice(range.clone(), rows.iter().inspect(|_| examined()).cloned());
        let entry = self.entries.get_mut(&identity).expect("edited stream");
        entry.rows.end = entry.rows.end + new - old;
        let position = entry.position;
        if new != old {
            for (_, identity) in self
                .order
                .range((Bound::Excluded(position), Bound::Unbounded))
            {
                examined();
                let other = self.entries.get_mut(identity).expect("ordered stream");
                other.rows = other.rows.start + new - old..other.rows.end + new - old;
            }
        }
        let changed = range.start;
        self.patches.push(Patch { range, rows });
        changed
    }

    #[cfg(test)]
    pub(super) fn forget_head_identity(&mut self, position: u64) {
        // Negative control: trust the incoming position and lose the retained
        // payload fingerprint, disabling both replacement identity checks.
        for entry in self.entries.values_mut() {
            entry.decoder.prefix = None;
            entry.position = (position, 0);
        }
        self.order = self
            .entries
            .iter()
            .map(|(identity, entry)| (entry.position, *identity))
            .collect();
        self.visible = self
            .entries
            .values()
            .filter(|entry| !entry.rows.is_empty())
            .map(|entry| entry.position)
            .collect();
    }

    #[cfg(test)]
    pub(super) fn pending_len(&self) -> usize {
        self.entries
            .values()
            .map(|entry| entry.decoder.pending_len)
            .sum()
    }
}

fn examined() {
    #[cfg(feature = "test-support")]
    crate::test_support::task_volume_item_examined();
}
