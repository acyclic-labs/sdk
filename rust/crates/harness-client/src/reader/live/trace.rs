//! Borrowed request-span file values, with the original Reader owning IO/follows.
use super::{Custody, Error, Reader, Source, Want};
use acyclic_stream::{
    RetainedRecord, StreamPath,
    trace::{Mechanism, QueryError, QuerySnapshot, TraceQuery},
};
use std::{collections::BTreeMap, num::NonZeroU32};

/// Self-scoped public file view; its backing is the admitted trace stream.
pub const PATH: &str = "live/agents/self/trace.json";

/// A read-only projection over one explicitly selected original source.
pub struct File {
    source: Source,
    custody: Custody,
    query: TraceQuery<()>,
    from: u64,
    next: u64,
    rejected: u128,
}

/// Original reader or projection failure. Missing coordinates are not replaced
/// with a selected stream name or its highest-commit anchor.
#[derive(Debug)]
pub enum ReadError<'a> {
    /// Original reader admission, gap or discontinuity.
    Source(Error<'a>),
    /// This cached row lacks its original immutable publication coordinates.
    MissingEvidence {
        /// Original selected cursor position without immutable coordinates.
        sequence: u64,
    },
    /// Original decoder/invariant cause and immutable publication.
    Projection(QueryError),
}

impl std::fmt::Display for ReadError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(error) => std::fmt::Display::fmt(error, f),
            Self::MissingEvidence { sequence } => write!(
                f,
                "trace row {sequence} has no original publication evidence"
            ),
            Self::Projection(error) => std::fmt::Display::fmt(error, f),
        }
    }
}

impl std::error::Error for ReadError<'_> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(Error::Source(issue)) => Some(*issue),
            Self::Projection(error) => Some(error),
            _ => None,
        }
    }
}

impl File {
    /// Bind an authorized original source and selected starting window. Neither
    /// the public path nor constructing this view grants read authority.
    pub fn new<'a>(
        reader: &'a mut Reader,
        stream: StreamPath,
        from: u64,
        thresholds_ns: BTreeMap<Mechanism, u64>,
    ) -> Result<Self, Error<'a>> {
        let source = Source::new(stream);
        let custody = source.custody(reader)?;
        Ok(Self {
            source,
            custody,
            query: TraceQuery::new((), thresholds_ns),
            from,
            next: from,
            rejected: 0,
        })
    }

    /// Feed this into the Reader's existing follow selection for a live file.
    pub fn stream(&self) -> &StreamPath {
        &self.source.stream
    }

    /// Plan only the next requested page on the original Reader.
    pub fn want(&self, count: NonZeroU32) -> Result<Want, Error<'static>> {
        self.source.want_page(self.next, count)
    }

    /// Borrow the current public value without IO or an aggregate history scan.
    pub fn value<'a>(&'a self, reader: &'a Reader) -> Result<QuerySnapshot<'a>, Error<'a>> {
        self.custody.check(reader, &self.source)?;
        Ok(self.query.snapshot(
            self.from,
            self.next,
            self.source.observed_tail(reader)?,
            self.rejected,
        ))
    }

    /// Shared projection, including lazy access to active original evidence.
    pub fn query<'a>(&'a self, reader: &'a Reader) -> Result<&'a TraceQuery<()>, Error<'a>> {
        self.custody.check(reader, &self.source)?;
        self.source.observed_tail(reader)?;
        Ok(&self.query)
    }

    /// Decode at most this page from already loaded original rows. No IO is
    /// issued, bytes copied, follow created or source identity reconstructed.
    /// Valid earlier rows remain admitted if a later row is pending or malformed.
    pub fn advance<'a>(
        &'a mut self,
        reader: &'a Reader,
        count: NonZeroU32,
    ) -> Result<usize, ReadError<'a>> {
        self.custody
            .check(reader, &self.source)
            .map_err(ReadError::Source)?;
        let page = self
            .source
            .page(reader, self.next, count)
            .map_err(ReadError::Source)?;
        let mut admitted = 0;
        for (sequence, row) in page {
            let row = row.map_err(ReadError::Source)?;
            let original = row
                .evidence()
                .ok_or(ReadError::MissingEvidence { sequence })?;
            let evidence = RetainedRecord {
                commit_id: original.commit,
                path: original.origin.clone(),
                sequence: original.sequence,
            };
            self.next = sequence
                .checked_add(1)
                .ok_or(ReadError::Source(Error::Range))?;
            if let Err(error) = self.query.admit(sequence, evidence, row.as_ref()) {
                self.rejected += 1;
                return Err(ReadError::Projection(error));
            }
            admitted += 1;
        }
        Ok(admitted)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
