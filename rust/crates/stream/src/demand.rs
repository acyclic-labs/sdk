//! Explicit, bounded stream dependencies shared by readers and projections.
use crate::StreamPath;

/// Protocol-sized requests covering exactly one logical listing page. Offsets
/// advance by the requested span even when authorization hides returned rows.
/// No allocation or request is performed until the consumer advances it.
pub fn listing_chunks(
    index: u32,
    limit: u32,
) -> Result<impl Iterator<Item = (u64, u32)>, crate::StreamError> {
    if limit == 0 {
        return Err(crate::StreamError::InvalidArgument);
    }
    let start = u64::from(index) * u64::from(limit);
    Ok((0..limit).step_by(crate::MAX_ITEMS).map(move |from| {
        (
            start + u64::from(from),
            (limit - from).min(crate::MAX_ITEMS as u32),
        )
    }))
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
