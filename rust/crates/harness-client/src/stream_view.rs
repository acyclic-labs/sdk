use crate::reader::backing::BackingPool;
use acyclic_stream::{StreamError, StreamPath, StreamWatermarks, wire};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, VecDeque},
    mem::size_of,
};

struct LatestEntry {
    value: wire::SessionLatestValue,
    charge: usize,
    next: u64,
    payload: usize,
}

struct RecordEntry {
    record: wire::Record,
    charge: usize,
    payload: usize,
}

/// Record-count page window for one selected stream. Eviction only releases
/// display residency: Streams retains history for paged backfill. A delivered
/// record or latest value is never refused for its size.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamViewPolicy {
    records: usize,
}

impl StreamViewPolicy {
    /// Select a nonzero page size without reserving storage for unseen records.
    pub fn new(records: usize) -> Result<Self, StreamError> {
        if records == 0 {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self { records })
    }
}

impl Default for StreamViewPolicy {
    fn default() -> Self {
        Self { records: 128 }
    }
}

/// One borrowed, bounded tail of canonical records, shared by native and WASM.
/// Eviction advances `first_sequence`; it never changes the delivery cursor or
/// silently discards a transport delivery. Every delivery still reaches callers.
/// Historical backfill belongs to the provider's page window, not this live tail.
pub struct StreamView {
    policy: StreamViewPolicy,
    records: VecDeque<RecordEntry>,
    latest: BTreeMap<Bytes, LatestEntry>,
    /// Delivery cursors are strictly increasing, so they also order eviction.
    /// Keys share their one compact allocation with the corresponding value.
    latest_order: BTreeMap<u64, Bytes>,
    #[cfg(test)]
    latest_probes: std::sync::atomic::AtomicUsize,
    // Per-entry charges can count the same shared allocation many times,
    // exceeding the address space without retaining that many physical bytes.
    bytes: u128,
    payload_bytes: u128,
    backings: BackingPool,
    next: u64,
    revision: u64,
    watermarks: StreamWatermarks,
    filtered: bool,
    /// The followed stream an unspelled record origin names.
    followed: Option<StreamPath>,
}

impl StreamView {
    /// `filtered` folds a follow whose provider omits records outside its
    /// selection: records and watermarks may then move the cursor forward
    /// past skipped sequences, but never backward.
    pub(crate) fn new(
        policy: StreamViewPolicy,
        next: u64,
        revision: u64,
        filtered: bool,
        followed: Option<StreamPath>,
        backings: BackingPool,
    ) -> Self {
        Self {
            policy,
            records: VecDeque::new(),
            latest: BTreeMap::new(),
            latest_order: BTreeMap::new(),
            #[cfg(test)]
            latest_probes: std::sync::atomic::AtomicUsize::new(0),
            bytes: 0,
            payload_bytes: 0,
            backings,
            next,
            revision,
            watermarks: StreamWatermarks {
                accepted: next,
                durable: 0,
            },
            filtered,
            followed,
        }
    }

    /// Borrows only the retained visible records; no joined history allocation.
    pub fn records(&self) -> impl ExactSizeIterator<Item = &wire::Record> {
        self.records.iter().map(|entry| &entry.record)
    }

    /// Last delivered opaque value for a key in this view's retained page.
    pub fn latest_value(&self, key: &[u8]) -> Option<&wire::SessionLatestValue> {
        #[cfg(test)]
        self.latest_probes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.latest.get(key).map(|entry| &entry.value)
    }

    #[cfg(test)]
    pub(crate) fn take_latest_probes(&self) -> usize {
        self.latest_probes
            .swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    /// Borrows one displayed record without copying its siblings.
    #[must_use]
    pub fn record(&self, index: usize) -> Option<&wire::Record> {
        self.records.get(index).map(|entry| &entry.record)
    }

    /// Exclusive end of this disposable fold, not a new recovery ledger.
    #[must_use]
    pub fn next_sequence(&self) -> u64 {
        self.next
    }

    /// First retained sequence, advancing when the display tail evicts a record.
    #[must_use]
    pub fn first_sequence(&self) -> u64 {
        self.records
            .front()
            .map_or(self.next, |entry| entry.record.sequence)
    }

    /// Session-wide revision, including replacement views, so a deselect/reselect
    /// cannot make a stale renderer cache look current again.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Resident entry bytes, including original payload capacity.
    /// Shared allocations count once in the enclosing session's residency total.
    /// Repeated shared charges saturate this per-entry total at `usize::MAX`.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        usize::try_from(self.bytes).unwrap_or(usize::MAX)
    }

    pub(crate) fn metadata_bytes(&self) -> usize {
        // Metadata is separately allocated per entry and fits the address space.
        usize::try_from(self.bytes - self.payload_bytes).unwrap_or(usize::MAX)
    }

    /// Accepted/durable watermarks are never inferred from a missing delivery.
    #[must_use]
    pub fn watermarks(&self) -> StreamWatermarks {
        self.watermarks
    }

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

    pub(crate) fn apply(
        &mut self,
        frame: &acyclic_stream::session::Frame,
        revision: u64,
    ) -> Result<(), StreamError> {
        match frame.wire().body.as_ref() {
            Some(wire::session_frame::Body::Data(data)) => {
                self.check_watermarks(data.accepted, data.durable)?;
                if let Some(wire::session_data::Item::LatestValue(value)) = data.item.as_ref() {
                    let after = data.after.as_ref().ok_or(StreamError::InvalidResponse)?;
                    if !self.filtered
                        || !self.records.is_empty()
                        || value.key.is_empty()
                        || after.next <= self.next
                        || after.next > data.accepted
                    {
                        return Err(StreamError::InvalidResponse);
                    }
                    let metadata = size_of::<(Bytes, LatestEntry)>()
                        + size_of::<(u64, Bytes)>()
                        + value.key.len();
                    let payload = frame.original_record().map_or_else(
                        || acyclic_stream::OwnedBytes::from(value.value.clone()),
                        |record| record.value.clone(),
                    );
                    let payload = self.backings.retain(payload, None, value.value.len());
                    let capacity = payload.backing().capacity();
                    let charge = metadata + capacity;
                    #[cfg(test)]
                    self.latest_probes
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if let Some(old) = self.latest.remove(value.key.as_ref()) {
                        self.latest_order.remove(&old.next);
                        self.bytes -= old.charge as u128;
                        self.payload_bytes -= old.payload as u128;
                    }
                    while self.latest.len() >= self.policy.records {
                        let (_, key) = self
                            .latest_order
                            .pop_first()
                            .ok_or(StreamError::InvalidResponse)?;
                        let old = self
                            .latest
                            .remove(key.as_ref())
                            .ok_or(StreamError::InvalidResponse)?;
                        self.bytes -= old.charge as u128;
                        self.payload_bytes -= old.payload as u128;
                    }
                    let key = Bytes::copy_from_slice(&value.key);
                    self.latest_order.insert(after.next, key.clone());
                    self.latest.insert(
                        key.clone(),
                        LatestEntry {
                            value: wire::SessionLatestValue {
                                key,
                                value: Bytes::from_owner(payload),
                                attention: value.attention,
                            },
                            charge,
                            next: after.next,
                            payload: capacity,
                        },
                    );
                    self.bytes += charge as u128;
                    self.payload_bytes += capacity as u128;
                    self.next = after.next;
                    self.revision = revision;
                    self.advance_watermarks(data.accepted, data.durable);
                    return Ok(());
                }
                let Some(wire::session_data::Item::Record(record)) = data.item.as_ref() else {
                    return Err(StreamError::InvalidResponse);
                };
                if !self.latest.is_empty() {
                    return Err(StreamError::InvalidResponse);
                }
                let next = record
                    .sequence
                    .checked_add(1)
                    .ok_or(StreamError::InvalidResponse)?;
                let after = data.after.as_ref().ok_or(StreamError::InvalidResponse)?;
                let skipped = if self.filtered {
                    record.sequence < self.next
                } else {
                    record.sequence != self.next
                };
                if skipped || after.next != next || next > data.accepted {
                    return Err(StreamError::InvalidResponse);
                }
                // Use the canonical record/header validator, including its size
                // bounds, before retaining anything from the delivery.
                let decoded = frame.decode_record(self.followed.as_ref())?;
                let (mut owned, metadata) = retain_metadata(record, frame.original_record());
                let payload = decoded.value;
                let payload = self.backings.retain(payload, None, record.value.len());
                let capacity = payload.backing().capacity();
                let charge = metadata + capacity;
                owned.value = Bytes::from_owner(payload);
                while self.records.len() >= self.policy.records {
                    if let Some(old) = self.records.pop_front() {
                        self.bytes -= old.charge as u128;
                        self.payload_bytes -= old.payload as u128;
                    }
                }
                self.records.push_back(RecordEntry {
                    record: owned,
                    charge,
                    payload: capacity,
                });
                self.bytes += charge as u128;
                self.payload_bytes += capacity as u128;
                self.next = next;
                self.revision = revision;
                self.advance_watermarks(data.accepted, data.durable);
            }
            Some(wire::session_frame::Body::Watermark(mark)) => {
                self.check_watermarks(mark.accepted, mark.durable)?;
                // An unfiltered ALL follow cannot skip records on a watermark;
                // a filtered one advances past records outside its selection.
                if let Some(after) = mark.after.as_ref() {
                    if after.next < self.next || (!self.filtered && after.next != self.next) {
                        return Err(StreamError::InvalidResponse);
                    }
                    self.next = after.next;
                }
                self.advance_watermarks(mark.accepted, mark.durable);
                self.revision = revision;
            }
            _ => {}
        }
        Ok(())
    }
}

fn retain_metadata(
    record: &wire::Record,
    original: Option<&acyclic_stream::Record>,
) -> (wire::Record, usize) {
    let mut bytes = size_of::<RecordEntry>();
    let mut keys = Vec::with_capacity(record.keys.len());
    let mut attributes = Vec::with_capacity(record.attributes.len());
    bytes += std::mem::size_of_val(keys.spare_capacity_mut());
    bytes += std::mem::size_of_val(attributes.spare_capacity_mut());
    // Charge the retained containers, rather than shrinking them through a
    // box solely to make their capacity agree with their logical length.
    let origin = record.origin.clone();
    bytes += origin.capacity();
    bytes += record.commit_id.len();
    for (index, key) in record.keys.iter().enumerate() {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        tests::visit_metadata();
        bytes += key.len();
        keys.push(retain_header(
            key,
            original.and_then(|record| record.headers.keys().get(index)),
        ));
    }
    for (index, attribute) in record.attributes.iter().enumerate() {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        tests::visit_metadata();
        let name = attribute.name.clone();
        bytes += name.capacity();
        bytes += attribute.value.len();
        attributes.push(wire::Attribute {
            name,
            value: retain_header(
                &attribute.value,
                original.and_then(|record| {
                    record
                        .headers
                        .attributes()
                        .get(index)
                        .map(|attribute| attribute.value)
                }),
            ),
        });
    }
    let owned = wire::Record {
        sequence: record.sequence,
        value: Bytes::new(),
        // Native frame construction already owns this exact fixed-size copy.
        commit_id: original.map_or_else(
            || Bytes::copy_from_slice(&record.commit_id),
            |_| record.commit_id.clone(),
        ),
        committed_at_micros: record.committed_at_micros,
        origin,
        keys,
        attributes,
    };
    (owned, bytes)
}

fn retain_header(bytes: &Bytes, original: Option<&acyclic_stream::OwnedBytes>) -> Bytes {
    // Only the immutable frame's canonical record can attest this backing.
    // Sharing a full allocation pays exactly the existing header byte charge;
    // unknown capacity or excess backing still requires a compact copy.
    if original.is_some_and(|original| original.retained_capacity() == Some(bytes.len())) {
        bytes.clone()
    } else {
        Bytes::copy_from_slice(bytes)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
