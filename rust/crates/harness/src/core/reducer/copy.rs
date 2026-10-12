use super::*;

impl Reducer {
    /// Copies the live projection using the caller's optional ledger.
    pub(crate) fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Reducer> {
        inspect_metadata(&(&self.authority, &self.projection, &self.events), work)?;
        let bytes = if work.is_some() {
            self.projection
                .owned_bytes(&self.authority, self.events.iter(), self.events.capacity())?
        } else {
            0
        };
        if let Some(io) = work.as_deref() {
            io.admit_copy(bytes)?;
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, bytes)?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.copy_bytes(bytes)?;
        }
        let (conversation, retained) = self.projection.conversation.copy(quota.work(), None)?;
        quota.adopt(retained)?;
        let verifier = self.authority_verifier.copy(quota.work())?;
        quota.adopt(verifier.logical_owned_bytes()?)?;
        let output = Reducer {
            authority: self.authority.clone(),
            authority_verifier: verifier,
            schemas: self.schemas.clone(),
            revision: self.revision,
            projection: self.projection.copy(conversation, ()),
            events: self.events.clone(),
            resident_event_limit: self.resident_event_limit,
            resident_terminal_effect_limit: self.resident_terminal_effect_limit,
        };
        quota.transfer();
        Ok(output)
    }
}
