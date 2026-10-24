use super::*;

/// A frame's values are destroyed before their original allocation custody.
pub(super) struct Pending {
    pub(super) delta: Delta,
    pub(super) publication: Publication,
    encoded_len: usize,
}

impl Pending {
    pub(super) fn new(sequence: u64, meter: Option<Arc<dyn NativeReadMeter>>) -> Self {
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

    pub(super) fn meter(&self) -> Option<Arc<dyn NativeReadMeter>> {
        self.publication
            .native
            .as_ref()
            .map(|(meter, _)| Arc::clone(meter))
    }

    pub(super) fn has_changes(&self) -> bool {
        !self.delta.buckets.is_empty()
            || !self.delta.objects.is_empty()
            || !self.delta.uploads.is_empty()
            || !self.delta.parts.is_empty()
            || !self.delta.receipts.is_empty()
    }

    pub(super) fn encoded_len(&self) -> usize {
        self.encoded_len
    }
}

impl Checkpoint {
    pub(super) fn append(
        &mut self,
        build: impl FnOnce(&mut Publication) -> Result<Change, NativeObjectsError>,
    ) -> Result<(), NativeObjectsError> {
        // One incoming item can outlive a flush of the preceding frame. Its
        // custody stays separate until the new frame accepts it.
        let mut custody = Publication::new(self.pending.meter());
        let item = build(&mut custody)?;
        let item_len = item.encoded_len();
        if self.pending.has_changes()
            && self
                .pending
                .encoded_len()
                .saturating_add(item_len.saturating_add(12))
                > PACK_LIMIT
        {
            self.flush()?;
        }
        match (&mut self.pending.publication.native, &mut custody.native) {
            (Some((_, retained)), Some((_, incoming))) => retained.absorb(incoming)?,
            (None, None) => (),
            _ => return Err(Error::from(Unavailable).into()),
        }
        // Tags 3..=7 each use one byte, followed by the item's length and body.
        let encoded_len = self
            .pending
            .encoded_len
            .checked_add(1)
            .and_then(|length| {
                length.checked_add(prost::encoding::encoded_len_varint(item_len as u64))
            })
            .and_then(|length| length.checked_add(item_len))
            .ok_or(Error::from(QuotaExceeded))?;
        item.insert(&mut self.pending.delta, &mut self.pending.publication)?;
        self.pending.encoded_len = encoded_len;
        Ok(())
    }
}
