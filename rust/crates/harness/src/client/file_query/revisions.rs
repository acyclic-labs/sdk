//! Bounded accepted-change history for disposable renderer cursors.
use crate::task_volume::{Change, FileKey};
use std::collections::VecDeque;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Revisions {
    floor: u64,
    entries: VecDeque<(u64, FileKey)>,
}
impl Revisions {
    pub fn new(position: u64) -> Self {
        Self {
            floor: position,
            entries: VecDeque::new(),
        }
    }
    pub fn record(&mut self, change: &Change, capacity: usize) {
        for key in change
            .file
            .iter()
            .chain(&change.files)
            .map(|file| &file.key)
            .chain(&change.removed)
        {
            if self.entries.len() == capacity
                && let Some((position, _)) = self.entries.pop_front()
            {
                self.floor = position;
            }
            self.entries
                .push_back((change.cursor.position, key.clone()));
        }
    }
    pub fn since(&self, after: u64) -> Option<impl Iterator<Item = &FileKey>> {
        if after < self.floor {
            return None;
        }
        let start = self
            .entries
            .partition_point(|(position, _)| *position <= after);
        Some(self.entries.range(start..).map(|(_, key)| key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_volume::{Cursor, SCHEMA};
    #[test]
    fn bounded_journal_falls_back_only_when_a_reader_loses_changes() {
        let mut changes = Revisions::new(1);
        for position in 2..=12 {
            changes.record(
                &Change {
                    cursor: Cursor {
                        volume: [1; 32],
                        epoch: uuid::Uuid::nil(),
                        schema: SCHEMA,
                        query: [2; 32],
                        position,
                        accepted: position,
                        durable: position,
                        commit: [3; 32],
                    },
                    file: None,
                    files: vec![],
                    removed: vec![FileKey::Goal],
                },
                4,
            );
            assert!(changes.entries.len() <= 4);
            assert_eq!(changes.since(position - 1).map(Iterator::count), Some(1));
            assert_eq!(changes.since(position).map(Iterator::count), Some(0));
        }
        assert!(changes.since(1).is_none());
        assert_eq!(changes.since(9).map(Iterator::count), Some(3));
    }
}
