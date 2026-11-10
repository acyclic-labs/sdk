//! Explicit, bounded stream dependencies shared by readers and projections.
use crate::StreamPath;

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
        /// Relative stream whose last record each child row carries.
        tail: Option<StreamPath>,
        /// A stream committed with every change to this parent's children
        /// (an agent's status, written by the step that spawns a child). A
        /// cached page revalidates once this stream's tail moves past the
        /// tail seen when the page was requested.
        witness: Option<StreamPath>,
    },
}
