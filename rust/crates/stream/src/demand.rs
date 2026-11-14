//! Explicit, bounded stream dependencies shared by readers and projections.
use crate::StreamPath;

/// Bounded rank traversal of one logical listing page. Byte-shortened replies
/// resume within the span; authorization-hidden rows retain their coordinates.
pub struct ListingCursor {
    offset: u64,
    remaining: u32,
}
impl ListingCursor {
    /// Selects a logical page without visiting earlier pages.
    pub fn new(index: u32, limit: u32) -> Result<Self, crate::StreamError> {
        if limit == 0 {
            return Err(crate::StreamError::InvalidArgument);
        }
        Ok(Self {
            offset: u64::from(index) * u64::from(limit),
            remaining: limit,
        })
    }
    /// Rank and protocol-sized credit for the next request, if any.
    pub fn request(&self) -> Option<(u64, u32)> {
        (self.remaining != 0).then(|| (self.offset, self.remaining.min(crate::MAX_ITEMS as u32)))
    }
    /// Advances by original provider progress, never projected visible length.
    pub fn advance(&mut self, page: &crate::ChildrenPage) -> Result<(), crate::StreamError> {
        let (_, limit) = self.request().ok_or(crate::StreamError::InvalidResponse)?;
        if page.children.len() > page.scanned as usize
            || page.scanned > limit
            || (page.next_after.is_some() && page.scanned == 0)
        {
            return Err(crate::StreamError::InvalidResponse);
        }
        self.offset += u64::from(page.scanned);
        self.remaining = if page.next_after.is_some() {
            self.remaining - page.scanned
        } else {
            0
        };
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
