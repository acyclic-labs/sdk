//! Explicit, bounded stream dependencies shared by readers and projections.
use crate::{ChildrenPage, CommitId, StreamError, StreamPath};

/// An indexed logical page, split only by transport capacity. The producer's
/// consumed rank preserves page identity even when authorization hides rows.
/// This cursor allocates nothing and performs no requests itself.
pub struct ListingPage {
    offset: u64,
    remaining: u32,
    version: Option<CommitId>,
}

impl ListingPage {
    /// Select one exact indexed span and, optionally, its hierarchy fence.
    pub fn new(index: u32, limit: u32, version: Option<CommitId>) -> Result<Self, StreamError> {
        if limit == 0 {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self {
            offset: u64::from(index) * u64::from(limit),
            remaining: limit,
            version,
        })
    }

    /// The next rank seek, result bound and original hierarchy fence.
    pub fn request(&self) -> Option<(u64, u32, Option<CommitId>)> {
        (self.remaining != 0).then_some((
            self.offset,
            self.remaining.min(crate::MAX_ITEMS as u32),
            self.version,
        ))
    }

    /// Consume only the producer's actual span, including hidden rows. A short
    /// byte-limited response never consumes the remainder of its request.
    pub fn advance(&mut self, page: &ChildrenPage) -> Result<(), StreamError> {
        let (_, bound, version) = self.request().ok_or(StreamError::InvalidResponse)?;
        if page.consumed > bound
            || page.children.len() > page.consumed as usize
            || (page.next_after.is_some() && page.consumed == 0)
            || version.is_some_and(|selected| selected != page.hierarchy_version)
        {
            return Err(StreamError::InvalidResponse);
        }
        self.offset = self
            .offset
            .checked_add(u64::from(page.consumed))
            .ok_or(StreamError::InvalidResponse)?;
        self.remaining -= page.consumed;
        self.version = Some(page.hierarchy_version);
        if page.next_after.is_none() {
            self.remaining = 0;
        }
        Ok(())
    }
}

/// One rendering need, stated each frame.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Want {
    /// Records `[from, to)` of a stream.
    Range {
        /// Stream.
        stream: StreamPath,
        /// First position.
        from: u64,
        /// Exclusive end.
        to: u64,
    },
    /// The last `count` records of a stream: its head.
    Last {
        /// Stream.
        stream: StreamPath,
        /// Records before the tail.
        count: u32,
    },
    /// Listing page `index` of a parent stream, projecting each child's
    /// `tail` relative stream head when given.
    Listing {
        /// `children_page` parent.
        parent: StreamPath,
        /// Page index.
        index: u32,
        /// Children per logical page; part of the page's identity and offset.
        limit: u32,
        /// Relative stream whose last record each child row carries.
        tail: Option<StreamPath>,
        /// A stream committed with every change to this parent's children
        /// (an agent's membership feed, written when a child is created). A
        /// cached page revalidates once this stream's tail moves past the
        /// tail seen when the page was requested. If the witness is absent,
        /// the page is cached without a freshness position and its original
        /// NotFound issue remains available to the caller.
        witness: Option<StreamPath>,
    },
}
