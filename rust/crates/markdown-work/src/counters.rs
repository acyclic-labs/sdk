use std::cell::Cell;

thread_local! {
    static SCAN: Cell<usize> = const { Cell::new(0) };
    static NODES: Cell<usize> = const { Cell::new(0) };
    static SPAN_COPIES: Cell<usize> = const { Cell::new(0) };
}

/// Cumulative work on this thread. Subtract snapshots around one `push`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Counts {
    /// Bytes supplied to parser passes and explicit scans.
    pub rescanned_bytes: usize,
    /// Parser event nodes and reference-definition nodes visited.
    pub reparsed_nodes: usize,
    /// Span payload bytes copied by construction, append and clone.
    pub span_copy_bytes: usize,
}

/// Snapshot the caller's counters without resetting another parser's work.
pub fn snapshot() -> Counts {
    Counts {
        rescanned_bytes: SCAN.get(),
        reparsed_nodes: NODES.get(),
        span_copy_bytes: SPAN_COPIES.get(),
    }
}

impl std::ops::Sub for Counts {
    type Output = Self;

    fn sub(self, before: Self) -> Self {
        Self {
            rescanned_bytes: self.rescanned_bytes - before.rescanned_bytes,
            reparsed_nodes: self.reparsed_nodes - before.reparsed_nodes,
            span_copy_bytes: self.span_copy_bytes - before.span_copy_bytes,
        }
    }
}

/// Account for a parser input or explicit byte scan.
pub fn scan(bytes: usize) {
    SCAN.set(SCAN.get() + bytes);
}

/// Account for one parser event or reference-definition node.
pub fn node() {
    NODES.set(NODES.get() + 1);
}

/// Account for span payload bytes copied into rendered output.
pub fn span_copy(bytes: usize) {
    SPAN_COPIES.set(SPAN_COPIES.get() + bytes);
}
