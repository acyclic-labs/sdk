//! Original monotonic coordinates for offline research projections.
use std::time::Instant;

/// The collector's original start and terminal observations. These retain no
/// operation owner. They never imply a Unix timestamp or cross-machine order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationInterval {
    /// Original operation creation observation.
    pub started: Instant,
    /// Original explicit completion, or incomplete-close observation.
    pub finished: Instant,
}

/// One research producer's original clock origin. The producer supplies a
/// distinct domain for each origin. This identifies clocks, not operations;
/// the original admission Binding remains the operation join authority.
/// Capture this once before installing the collector, never once per operation.
/// No wall-clock placement or synchronization accuracy is inferred.
#[derive(Clone, Copy, Debug)]
pub struct IntervalOrigin {
    /// Opaque original clock-domain identity, not an agent or metric label.
    pub domain: [u8; 16],
    /// Original monotonic origin in the same process as the collector.
    pub instant: Instant,
}

/// Fixed coordinates suitable for the existing research exporter. Durations
/// use the full nanosecond representation; no history or origin map is retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct IntervalCoordinates {
    /// The original producer's domain; coordinates in other domains cannot be
    /// ordered without separate original clock synchronization evidence.
    pub domain: [u8; 16],
    /// Start relative to the original monotonic origin.
    pub started_ns: u128,
    /// Terminal observation relative to the same origin.
    pub finished_ns: u128,
}

impl IntervalOrigin {
    /// Projects two original observations without a clock read, allocation,
    /// syscall, wait, parent lookup or retained history. Invalid coordinates
    /// remain typed errors rather than plausible zero-duration operations.
    pub fn project(
        self,
        interval: OperationInterval,
    ) -> Result<IntervalCoordinates, IntervalError> {
        let started = interval
            .started
            .checked_duration_since(self.instant)
            .ok_or(IntervalError::BeforeOrigin)?;
        let elapsed = interval
            .finished
            .checked_duration_since(interval.started)
            .ok_or(IntervalError::Reversed)?;
        Ok(IntervalCoordinates {
            domain: self.domain,
            started_ns: started.as_nanos(),
            finished_ns: started.as_nanos() + elapsed.as_nanos(),
        })
    }
}

/// The supplied origin cannot truthfully place this original interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntervalError {
    /// The capture origin was observed after the operation started.
    BeforeOrigin,
    /// The terminal observation precedes operation creation.
    Reversed,
}

impl std::fmt::Display for IntervalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::BeforeOrigin => {
                "project interval: use its original origin preceding operation creation"
            }
            Self::Reversed => {
                "project interval: preserve the original start and terminal observations"
            }
        })
    }
}
impl std::error::Error for IntervalError {}
