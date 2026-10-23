//! Platform session records reuse the same file-query fold as native follows.
use super::FileViews;
use crate::task_volume::{Change, Error, FileValue, Result};
use acyclic_stream::{StreamWatermarks, wire, wire_codec};

impl FileViews {
    pub(crate) fn session_change(&self, data: &wire::SessionData) -> Result<Change> {
        let Some(wire::session_data::Item::Record(record)) = &data.item else {
            return Err(Error::InvalidSegment);
        };
        let after = data.after.as_ref().ok_or(Error::InvalidSegment)?;
        let record = wire_codec::record(record.clone())?;
        if record.sequence.checked_add(1) != Some(after.next)
            || data.accepted < after.next
            || data.durable > data.accepted
        {
            return Err(Error::ResetRequired);
        }
        let mut cursor = self.cursor().clone();
        cursor.position = after.next;
        cursor.commit = *record.commit_id.as_bytes();
        // Control metadata may overtake a credit-blocked data frame. Consuming
        // that older frame must preserve the newer confirmed watermarks.
        cursor.accepted = cursor.accepted.max(data.accepted);
        cursor.durable = cursor.durable.max(data.durable);
        Ok(Change {
            cursor,
            file: FileValue::from_record(&record)?,
            files: Vec::new(),
            removed: Vec::new(),
        })
    }

    pub(crate) fn session_watermark(&mut self, value: &wire::SessionWatermark) -> Result<()> {
        self.watermark(StreamWatermarks {
            accepted: value.accepted,
            durable: value.durable,
        })
    }
}
