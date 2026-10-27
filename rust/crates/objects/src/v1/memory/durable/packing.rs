use super::snapshot::Change;
use super::*;

/// A frame's values are destroyed before their original allocation custody.
pub(in crate::v1::memory) struct Pending {
    pub(in crate::v1::memory) delta: Delta,
    pub(in crate::v1::memory) publication: Publication,
    encoded_len: usize,
}

impl Pending {
    pub(in crate::v1::memory) fn new(
        sequence: u64,
        meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> Self {
        let delta = Delta {
            sequence,
            ..Default::default()
        };
        let encoded_len = delta.encoded_len();
        Self {
            delta,
            publication: Publication::new(meter),
            encoded_len,
        }
    }

    pub(in crate::v1::memory) fn meter(&self) -> Option<Arc<dyn NativeReadMeter>> {
        self.publication
            .native
            .as_ref()
            .map(|(meter, _)| Arc::clone(meter))
    }

    pub(in crate::v1::memory) fn has_changes(&self) -> bool {
        !self.delta.buckets.is_empty()
            || !self.delta.objects.is_empty()
            || !self.delta.uploads.is_empty()
            || !self.delta.parts.is_empty()
            || !self.delta.receipts.is_empty()
    }

    pub(in crate::v1::memory) fn encoded_len(&self) -> usize {
        self.encoded_len
    }
}

impl Pending {
    pub(in crate::v1::memory) fn should_flush(&self, item_len: usize) -> bool {
        self.has_changes()
            && self
                .encoded_len()
                .saturating_add(item_len.saturating_add(12))
                > PACK_LIMIT
    }
    pub(in crate::v1::memory) fn insert(
        &mut self,
        item: Change,
        mut custody: Publication,
    ) -> Result<(), NativeObjectsError> {
        let item_len = item.encoded_len();
        match (&mut self.publication.native, &mut custody.native) {
            (Some((_, retained)), Some((_, incoming))) => retained.absorb(incoming)?,
            (None, None) => (),
            _ => return Err(Error::from(Unavailable).into()),
        }
        // Tags 3..=7 each use one byte, followed by the item's length and body.
        let encoded_len = self
            .encoded_len
            .checked_add(1)
            .and_then(|length| {
                length.checked_add(prost::encoding::encoded_len_varint(item_len as u64))
            })
            .and_then(|length| length.checked_add(item_len))
            .ok_or(Error::from(QuotaExceeded))?;
        item.insert(&mut self.delta, &mut self.publication)?;
        self.encoded_len = encoded_len;
        Ok(())
    }
}
