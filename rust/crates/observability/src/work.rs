//! Optional work totals supplied by the original operation producer.
use tracing::field::{Field, Visit};

/// The producer's latest declared totals before terminal completion.
/// Missing fields remain unknown, including a producer that records no totals.
/// These semantic units do not establish OS-wide or physical-call coverage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkSummary {
    /// Items examined in the producer's own accounting boundary.
    pub items: Option<u64>,
    /// Bytes accounted for by the producer, without inferring copies or I/O.
    pub bytes: Option<u64>,
    /// Producer-declared durability operations, not an OS syscall/drain count.
    pub durability: Option<u64>,
}

impl Visit for WorkSummary {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "work.items" => self.items = Some(value),
            "work.bytes" => self.bytes = Some(value),
            "work.durability" => self.durability = Some(value),
            _ => {}
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
