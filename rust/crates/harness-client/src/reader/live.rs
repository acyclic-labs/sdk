//! Bounded live file reads over the one [`Reader`].
//!
//! A caller supplies the original committed stream backing a resolved file and
//! uses its existing decoder before projecting the public file value.
//! This module does not resolve agent aliases or create a stream, writer, cache,
//! watcher or background service. File heads use one tail observation; JSONL
//! windows use the caller's range. Parts directories use ordinary
//! [`super::Want::Listing`] and this same source for each selected part.

use super::{Reader, RecordBytes, Want};
use acyclic_stream::StreamPath;
use std::num::NonZeroU32;

#[cfg(test)]
mod tests;

/// One public file's original committed record source.
#[derive(Clone, Debug)]
pub struct Source {
    stream: StreamPath,
}

/// A live value must not turn a missing source, gap or refused read into zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The original Reader has no source custody.
    #[error("live source is unreadable; recovery: restore the original reader's source admission")]
    Unreadable,
    /// Preserve the original reader's authorization or retention discontinuity.
    #[error(
        "live source changed: {0:?}; recovery: restore admission or reopen at the original source's retained cut"
    )]
    Source(super::ReadIssue),
    /// Drive the already planned Reader request or its original follow event.
    #[error("live source is not loaded; recovery: drive the shared Reader for this file's want")]
    Pending,
    /// A known empty stream is distinct from an unloaded stream.
    #[error("live source is empty; recovery: wait on the original producer's stream follow")]
    Empty,
    /// A JSONL range cannot move backwards or overflow.
    #[error(
        "live range is outside the observed source; recovery: use a cursor at or before its observed tail"
    )]
    Range,
}

impl Source {
    /// Bind the original stream backing this file. An agent
    /// name, request digest or phase is not used to reconstruct its identity.
    pub const fn new(stream: StreamPath) -> Self {
        Self { stream }
    }

    /// Plan a snapshot file, such as status, counters, budget or context.
    pub fn want(&self) -> Want {
        Want::Last {
            stream: self.stream.clone(),
            count: 1,
        }
    }

    /// Plan exactly the caller's JSONL window; there is no hidden page size.
    pub fn want_page(&self, from: u64, count: NonZeroU32) -> Result<Want, Error> {
        Ok(Want::Range {
            stream: self.stream.clone(),
            from,
            to: from
                .checked_add(u64::from(count.get()))
                .ok_or(Error::Range)?,
        })
    }

    /// Borrow the observed latest original record without IO, a payload copy
    /// or a second encoding. Decode using the original producer's format.
    pub fn latest<'a>(&self, reader: &'a Reader) -> Result<&'a RecordBytes, Error> {
        let tail = self.tail(reader)?;
        let sequence = tail.checked_sub(1).ok_or(Error::Empty)?;
        reader
            .cache()
            .record(&self.stream, sequence)
            .ok_or(Error::Pending)
    }

    /// Lazily borrow only the requested original records. The exclusive end is
    /// the original observed tail clipped to the supplied window. Reaching it
    /// is not producer completion: growth is delivered by the original follow.
    pub fn page<'a>(
        &'a self,
        reader: &'a Reader,
        from: u64,
        count: NonZeroU32,
    ) -> Result<Page<impl Iterator<Item = (u64, Option<&'a RecordBytes>)>>, Error> {
        let tail = self.tail(reader)?;
        if from > tail {
            return Err(Error::Range);
        }
        let end = from
            .checked_add(u64::from(count.get()))
            .ok_or(Error::Range)?
            .min(tail);
        Ok(Page {
            end,
            records: reader.cache().records(&self.stream, from, end),
        })
    }

    fn tail(&self, reader: &Reader) -> Result<u64, Error> {
        if !reader.readable() {
            return Err(Error::Unreadable);
        }
        if let Some(issue) = reader.issues.get(&self.stream) {
            return Err(Error::Source(*issue));
        }
        reader.cache().tail(&self.stream).ok_or(Error::Pending)
    }
}

/// One borrowed window. No page payload, prefix or decoded value is retained
/// beyond what its caller consumes. Work is proportional to requested records.
pub struct Page<I> {
    /// Exclusive original stream cursor, including gaps; it is not an ACK.
    pub end: u64,
    records: I,
}

impl<'a, I: Iterator<Item = (u64, Option<&'a RecordBytes>)>> Iterator for Page<I> {
    type Item = (u64, Result<&'a RecordBytes, Error>);

    fn next(&mut self) -> Option<Self::Item> {
        self.records
            .next()
            .map(|(sequence, bytes)| (sequence, bytes.ok_or(Error::Pending)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.records.size_hint()
    }
}
