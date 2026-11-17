//! Allocation-free arithmetic shared by epoch preparation and its proofs.

/// An item boundary supplied by the conversation writer after validating its
/// tool and reasoning pairs. An open exchange can never become a summary cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Boundary {
    /// More items belong to this exchange.
    Open,
    /// Every tool and reasoning pair in the prefix is complete.
    Closed,
}

/// Latest complete exchange in a single forward pass over the active epoch.
/// No history is retained or searched when a watermark is reached.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Cut {
    observed: u64,
    closed: u64,
}

impl Cut {
    /// Resumes after an already summarized prefix.
    #[must_use]
    pub const fn after(cursor: u64) -> Self {
        Self {
            observed: cursor,
            closed: cursor,
        }
    }

    /// Accepts exactly the next item. A gap, duplicate or exhausted cursor
    /// refuses without changing the selected cut.
    pub fn push(&mut self, end: u64, boundary: Boundary) -> bool {
        if self.observed.checked_add(1) != Some(end) {
            return false;
        }
        self.observed = end;
        if boundary == Boundary::Closed {
            self.closed = end;
        }
        true
    }

    /// Exclusive end of the latest complete exchange.
    #[must_use]
    pub const fn cursor(self) -> u64 {
        self.closed
    }
}

/// Conservative input-token bound since the last authoritative usage report.
/// Every appended encoded byte costs at most one token; the renderer supplies
/// the token overhead of the newly introduced seams separately.
#[must_use]
pub const fn token_bound(reported: u64, delta_bytes: u64, seam_tokens: u64) -> Option<u64> {
    let Some(with_bytes) = reported.checked_add(delta_bytes) else {
        return None;
    };
    with_bytes.checked_add(seam_tokens)
}

/// A request is dispatchable only with its entire output reservation available.
/// Subtraction avoids overflow even at the largest representable capacity.
#[must_use]
pub const fn fits(input: u64, output: u64, capacity: u64) -> bool {
    output <= capacity && input <= capacity - output
}

/// A summary may be shared only when its entire source range is inherited.
#[must_use]
pub const fn inherited(through: u64, common_prefix: u64) -> bool {
    through <= common_prefix
}
