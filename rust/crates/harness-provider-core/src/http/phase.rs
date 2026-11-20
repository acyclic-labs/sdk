//! Response-head observations use the process clock, independently of a
//! transport's admission/backoff clock. Publishing the start precedes I/O;
//! cancellation or an exchange without a response leaves that start unfinished.
use acyclic_harness_core::OperationId;
use acyclic_stream::trace::{Mechanism, Outcome, SpanEvent, SpanId, TraceRecord};
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};

/// The trace span name of a response-head observation.
pub const NAME: &str = "model.response_headers";
/// The public guarantee a response-head observation buys.
pub const GUARANTEE: &str = "receive_response_headers";

/// One response-head observation: its span identity and its start instant.
pub struct HeadersPhase {
    id: SpanId,
    started: u64,
}

fn now() -> Option<u64> {
    #[cfg(not(target_arch = "wasm32"))]
    let now = super::native_now();
    #[cfg(target_arch = "wasm32")]
    let now = acyclic_harness_core::http::browser::monotonic_now()?;
    u64::try_from(now.as_nanos()).ok()
}

impl HeadersPhase {
    /// Starts an observation, or `None` when the process clock is unavailable.
    pub fn new() -> Option<Self> {
        let started = now()?;
        static PROCESS: OnceLock<[u8; 16]> = OnceLock::new();
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let sequence = SEQUENCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .ok()?;
        Some(Self {
            id: SpanId {
                process: *PROCESS.get_or_init(|| OperationId::new().into_bytes()),
                sequence,
            },
            started,
        })
    }

    /// The record publishing this observation's start; it precedes the I/O.
    pub fn started(&self) -> TraceRecord {
        TraceRecord::V1(SpanEvent::Started {
            id: self.id,
            parent: None,
            name: NAME.into(),
            mechanism: Mechanism::RoundTrip,
            at_ns: self.started,
            guarantee: Some(GUARANTEE.into()),
        })
    }

    /// The record finishing this observation, or `None` when the clock went backwards.
    pub fn finished(&self) -> Option<TraceRecord> {
        let at_ns = now()?;
        if at_ns < self.started {
            return None;
        }
        Some(TraceRecord::V1(SpanEvent::Finished {
            id: self.id,
            at_ns,
            bytes: None,
            result: Outcome::Succeeded,
        }))
    }
}
