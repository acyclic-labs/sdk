//! Optional work totals supplied by the original operation producer.
use tracing::field::{Field, Visit};

/// The producer's latest declared totals before terminal completion.
/// Missing fields remain unknown, including a producer that records no totals.
/// A later value outside the unsigned 64-bit contract replaces an earlier total
/// with unknown; it cannot preserve a stale count or be parsed from text.
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

impl WorkSummary {
    fn field(&mut self, field: &Field) -> Option<&mut Option<u64>> {
        match field.name() {
            "work.items" => Some(&mut self.items),
            "work.bytes" => Some(&mut self.bytes),
            "work.durability" => Some(&mut self.durability),
            _ => None,
        }
    }
}

impl Visit for WorkSummary {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if let Some(total) = self.field(field) {
            *total = Some(value);
        }
    }

    fn record_debug(&mut self, field: &Field, _: &dyn std::fmt::Debug) {
        if let Some(total) = self.field(field) {
            *total = None;
        }
    }
}
