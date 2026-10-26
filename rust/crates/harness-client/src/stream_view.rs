use acyclic_stream::{StreamError, StreamWatermarks, wire, wire_codec};
use bytes::Bytes;
use std::{collections::VecDeque, mem::size_of};

/// Finite display-cache bounds for one selected stream. Transport credit and
/// recovery remain owned by Streams; these bounds apply to retained render data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamViewPolicy {
    records: usize,
    bytes: usize,
}

impl StreamViewPolicy {
    /// Refuses zero budgets and budgets above the client hard ceilings.
    pub fn new(records: usize, bytes: usize) -> Result<Self, StreamError> {
        if records == 0 || records > 1024 || bytes == 0 || bytes > 16 * 1024 * 1024 {
            return Err(StreamError::LimitExceeded);
        }
        Ok(Self { records, bytes })
    }
}

impl Default for StreamViewPolicy {
    fn default() -> Self {
        Self { records: 128, bytes: 1024 * 1024 }
    }
}

/// One borrowed, bounded tail of canonical records, shared by native and WASM.
/// Eviction advances `first_sequence`; it never changes the delivery cursor or
/// silently discards a transport delivery. Every delivery still reaches callers.
/// Historical backfill belongs to the provider's page window, not this live tail.
pub struct StreamView {
    policy: StreamViewPolicy,
    records: VecDeque<(wire::Record, usize)>,
    bytes: usize,
    next: u64,
    revision: u64,
    watermarks: StreamWatermarks,
}

impl StreamView {
    pub(crate) fn new(policy: StreamViewPolicy, next: u64, revision: u64) -> Self {
        Self {
            policy,
            records: VecDeque::new(),
            bytes: 0,
            next,
            revision,
            watermarks: StreamWatermarks { accepted: next, durable: 0 },
        }
    }

    /// Borrows only the retained visible records; no joined history allocation.
    pub fn records(&self) -> impl ExactSizeIterator<Item = &wire::Record> {
        self.records.iter().map(|(record, _)| record)
    }

    /// Borrows one displayed record without copying its siblings.
    #[must_use]
    pub fn record(&self, index: usize) -> Option<&wire::Record> {
        self.records.get(index).map(|(record, _)| record)
    }

    /// Exclusive end of this disposable fold, not a new recovery ledger.
    #[must_use]
    pub fn next_sequence(&self) -> u64 { self.next }

    /// First retained sequence, advancing when the display tail evicts a record.
    #[must_use]
    pub fn first_sequence(&self) -> u64 {
        self.records.front().map_or(self.next, |(record, _)| record.sequence)
    }

    /// Session-wide revision, including replacement views, so a deselect/reselect
    /// cannot make a stale renderer cache look current again.
    #[must_use]
    pub fn revision(&self) -> u64 { self.revision }

    /// Retained payload and record/header allocation charge.
    #[must_use]
    pub fn retained_bytes(&self) -> usize { self.bytes }

    /// Accepted/durable watermarks are never inferred from a missing delivery.
    #[must_use]
    pub fn watermarks(&self) -> StreamWatermarks { self.watermarks }

    fn check_watermarks(&self, accepted: u64, durable: u64) -> Result<(), StreamError> {
        if durable > accepted {
            return Err(StreamError::InvalidResponse);
        }
        Ok(())
    }

    fn advance_watermarks(&mut self, accepted: u64, durable: u64) {
        // Control metadata can overtake a credit-blocked data envelope. Its
        // older captured watermarks do not rewind the confirmed progress.
        self.watermarks.accepted = self.watermarks.accepted.max(accepted);
        self.watermarks.durable = self.watermarks.durable.max(durable);
    }

    pub(crate) fn apply(&mut self, frame: &wire::SessionFrame, revision: u64) -> Result<(), StreamError> {
        match frame.body.as_ref() {
            Some(wire::session_frame::Body::Data(data)) => {
                self.check_watermarks(data.accepted, data.durable)?;
                let Some(wire::session_data::Item::Record(record)) = data.item.as_ref() else {
                    return Err(StreamError::InvalidResponse);
                };
                let next = record.sequence.checked_add(1).ok_or(StreamError::InvalidResponse)?;
                let after = data.after.as_ref().ok_or(StreamError::InvalidResponse)?;
                if record.sequence != self.next || after.next != next || next > data.accepted {
                    return Err(StreamError::InvalidResponse);
                }
                // Use the canonical record/header validator, including its size
                // bounds, before retaining anything from the delivery.
                wire_codec::record(record.clone())?;
                let charge = record_charge(record)?;
                if charge > self.policy.bytes {
                    return Err(StreamError::LimitExceeded);
                }
                // Rehome shared byte slices: a tiny visible value cannot keep an
                // arbitrarily larger receive buffer alive behind its charge.
                let owned = compact_record(record);
                while self.records.len() >= self.policy.records || self.bytes > self.policy.bytes - charge {
                    if let Some((_, old_charge)) = self.records.pop_front() {
                        self.bytes -= old_charge;
                    }
                }
                self.records.push_back((owned, charge));
                self.bytes += charge;
                self.next = next;
                self.revision = revision;
                self.advance_watermarks(data.accepted, data.durable);
            }
            Some(wire::session_frame::Body::Watermark(mark)) => {
                self.check_watermarks(mark.accepted, mark.durable)?;
                // An unfiltered ALL follow cannot skip records on a watermark.
                if mark.after.as_ref().is_some_and(|after| after.next != self.next) {
                    return Err(StreamError::InvalidResponse);
                }
                self.advance_watermarks(mark.accepted, mark.durable);
                self.revision = revision;
            }
            _ => {}
        }
        Ok(())
    }
}

fn record_charge(record: &wire::Record) -> Result<usize, StreamError> {
    let mut charge = size_of::<(wire::Record, usize)>();
    for bytes in std::iter::once(record.value.len())
        .chain(std::iter::once(record.commit_id.len()))
        .chain(record.keys.iter().map(|key| size_of::<Bytes>() + key.len()))
        .chain(record.attributes.iter().map(|attribute| size_of::<wire::Attribute>() + attribute.name.len() + attribute.value.len()))
    {
        charge = charge.checked_add(bytes).ok_or(StreamError::LimitExceeded)?;
    }
    Ok(charge)
}

fn compact_record(record: &wire::Record) -> wire::Record {
    wire::Record {
        sequence: record.sequence,
        value: Bytes::copy_from_slice(&record.value),
        commit_id: Bytes::copy_from_slice(&record.commit_id),
        committed_at_micros: record.committed_at_micros,
        keys: record.keys.iter().map(|key| Bytes::copy_from_slice(key)).collect::<Vec<_>>().into_boxed_slice().into_vec(),
        attributes: record.attributes.iter().map(|attribute| wire::Attribute {
            name: attribute.name.as_str().into(),
            value: Bytes::copy_from_slice(&attribute.value),
        }).collect::<Vec<_>>().into_boxed_slice().into_vec(),
    }
}
