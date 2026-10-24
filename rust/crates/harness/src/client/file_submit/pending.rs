//! Pending outbox requests own their quota. An entry, its identity and its
//! payload bytes are admitted and retired together; no caller holds a counter.
use super::{FileClientError, FileClientLimits, Result};
use crate::{
    Error,
    task_volume::{Cursor, FileKey, Write},
};
use std::collections::{HashSet, VecDeque};

pub(super) struct Pending {
    pub(super) cursor: Cursor,
    pub(super) write: Write,
}

pub(super) struct PendingRequests {
    entries: VecDeque<Pending>,
    keys: HashSet<FileKey>,
    bytes: u64,
    commands: usize,
    limit: u64,
}

impl PendingRequests {
    pub(super) fn new(limits: FileClientLimits) -> Self {
        Self {
            entries: VecDeque::new(),
            keys: HashSet::new(),
            bytes: 0,
            commands: limits.commands,
            limit: limits.bytes,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn front(&self) -> Option<&Pending> {
        self.entries.front()
    }

    /// Check-only admission used before a caller copies a payload. Command
    /// capacity precedes byte capacity.
    pub(super) fn check(&self, bytes: u64) -> Result<()> {
        let result = self.fits(bytes).map(drop);
        #[cfg(test)]
        let result = tests::checked(result);
        result
    }

    fn fits(&self, bytes: u64) -> Result<u64> {
        if self.entries.len() >= self.commands {
            return Err(FileClientError::CommandQuota {
                limit: self.commands,
            });
        }
        self.bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= self.limit)
            .ok_or(FileClientError::ByteQuota { limit: self.limit })
    }

    /// Identity conflicts precede capacity; nothing changes on rejection.
    pub(super) fn admit(&mut self, cursor: Cursor, write: Write) -> Result<()> {
        let result = (|| {
            if self.keys.contains(&write.key) {
                return Err(Error::Conflict("outbox request identity conflict".into()).into());
            }
            let bytes = self.fits(write.bytes.len() as u64)?;
            self.keys.insert(write.key.clone());
            self.bytes = bytes;
            self.entries.push_back(Pending { cursor, write });
            Ok(())
        })();
        #[cfg(test)]
        let result = tests::admitted(self, result);
        result
    }

    /// Hands the oldest entry to the caller after releasing its quota.
    pub(super) fn retire_front(&mut self) -> Option<Pending> {
        let done = self.entries.pop_front()?;
        self.bytes -= done.write.bytes.len() as u64;
        self.keys.remove(&done.write.key);
        #[cfg(test)]
        let done = tests::retired(self, done);
        Some(done)
    }
}

#[cfg(test)]
mod tests;
