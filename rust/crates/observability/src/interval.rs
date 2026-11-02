//! Original monotonic coordinates for offline research projections.
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
/// Optional Unix placement requires an original [`UnixClockAnchor`]; neither
/// origin nor anchor certifies clock synchronization accuracy.
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

/// One original local wall-clock observation, bracketed by monotonic reads at
/// collector installation. The read window describes observation placement,
/// not OS clock resolution, absolute accuracy, later drift or clock steps.
/// Retain this once alongside the origin, not once per operation. Missing
/// observations and foreign domains cannot supply Unix timestamps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct UnixClockAnchor {
    /// Original origin's domain; never a cross-machine synchronization claim.
    pub domain: [u8; 16],
    /// Original SystemTime observation, in nanoseconds since the Unix epoch.
    pub observed_unix_ns: u128,
    /// Monotonic time between the reads surrounding that observation. The
    /// first read is the original IntervalOrigin.instant, at coordinate zero.
    pub read_window_ns: u128,
}

/// Local-clock extrapolation from an original anchor. Timestamps use the
/// midpoint of its read window; that window remains explicit evidence, not a
/// bound on absolute clock error. Synchronization, resolution and drift are
/// unknown. These estimates alone cannot order work in different domains.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct UnixIntervalEstimate {
    /// Original local time domain, preserved through projection.
    pub domain: [u8; 16],
    /// Estimated start since Unix epoch under the anchor's local-clock mapping.
    pub started_unix_ns: u128,
    /// Estimated terminal time under that same mapping.
    pub finished_unix_ns: u128,
    /// Original observation-placement window, without guessed clock accuracy.
    pub read_window_ns: u128,
}

impl IntervalOrigin {
    /// Opt into Unix placement once before installing a research collector.
    /// Exactly two monotonic reads bracket one wall-clock read. There are no
    /// allocations, tasks or per-operation reads. Use a distinct domain for
    /// this origin; retain both returned facts through the original export.
    pub fn capture_unix(domain: [u8; 16]) -> Result<(Self, UnixClockAnchor), IntervalError> {
        let instant = Instant::now();
        let observed = SystemTime::now();
        let finished = Instant::now();
        let observed_unix_ns = observed
            .duration_since(UNIX_EPOCH)
            .map_err(|_| IntervalError::BeforeUnixEpoch)?
            .as_nanos();
        let read_window_ns = finished
            .checked_duration_since(instant)
            .ok_or(IntervalError::Reversed)?
            .as_nanos();
        Ok((
            Self { domain, instant },
            UnixClockAnchor {
                domain,
                observed_unix_ns,
                read_window_ns,
            },
        ))
    }

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

impl UnixClockAnchor {
    /// Extrapolate the original local mapping with fixed arithmetic, no clocks,
    /// allocation, lookup or retained history. Reject a foreign domain rather
    /// than manufacturing placement from elapsed time or another collector.
    pub fn project(
        self,
        interval: IntervalCoordinates,
    ) -> Result<UnixIntervalEstimate, IntervalError> {
        if interval.domain != self.domain {
            return Err(IntervalError::ForeignDomain);
        }
        if interval.finished_ns < interval.started_ns {
            return Err(IntervalError::Reversed);
        }
        let map = |offset: u128| {
            let midpoint = self.read_window_ns / 2;
            if offset >= midpoint {
                self.observed_unix_ns.checked_add(offset - midpoint)
            } else {
                self.observed_unix_ns.checked_sub(midpoint - offset)
            }
            .ok_or(IntervalError::UnixRange)
        };
        Ok(UnixIntervalEstimate {
            domain: self.domain,
            started_unix_ns: map(interval.started_ns)?,
            finished_unix_ns: map(interval.finished_ns)?,
            read_window_ns: self.read_window_ns,
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
    /// Coordinates belong to a different original collector origin.
    ForeignDomain,
    /// The original OS wall-clock observation precedes the Unix epoch.
    BeforeUnixEpoch,
    /// The estimated Unix coordinates cannot be represented as u128.
    UnixRange,
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
            Self::ForeignDomain => "project Unix interval: use the anchor from its original domain",
            Self::BeforeUnixEpoch => {
                "capture Unix anchor: retain unavailable coverage for a clock before the Unix epoch"
            }
            Self::UnixRange => {
                "project Unix interval: retain unavailable coverage for unrepresentable coordinates"
            }
        })
    }
}
impl std::error::Error for IntervalError {}
