//! Copying the live reducer projection.
use super::*;

impl Reducer {
    /// Copies the live projection using the caller's optional ledger.
    pub(crate) fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Reducer> {
        self.native_custody
            .require_work(work, "native reducer cloning requires explicit native work")?;
        source_metadata_inspection_with_work(
            &(&self.authority, &self.projection, &self.events),
            work,
        )?;
        let mut bytes = 0;
        if work.is_some() {
            self.projection.visit(
                &self.authority,
                self.events.iter(),
                self.events.capacity(),
                |key, owned| {
                    if !matches!(key, NativeComponentKey::ConversationMessage(_)) {
                        bytes = owned_sum([bytes, owned])?;
                    }
                    Ok(())
                },
            )?;
            if let Some(io) = work.as_deref() {
                io.admit_copy(bytes)?;
            }
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, bytes)?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.copy_bytes(bytes)?;
        }
        let (conversation, retained) = self.projection.conversation.copy(quota.work(), None)?;
        quota.adopt(retained)?;
        let verifier = self.authority_verifier.copy(quota.work())?;
        quota.adopt(verifier.logical_owned_bytes()?)?;
        let mut output = Reducer {
            authority: self.authority.clone(),
            authority_verifier: verifier,
            schemas: self.schemas.clone(),
            revision: self.revision,
            projection: self.projection.copy(conversation, ()),
            events: self.events.clone(),
            resident_event_limit: self.resident_event_limit,
            resident_terminal_effect_limit: self.resident_terminal_effect_limit,
            native_custody: NativeComponentCustody::default(),
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if quota.work().as_deref().is_some_and(copying::is_native) {
            let retained = quota.retained();
            let io = quota.work().as_deref_mut().expect("native copy observer");
            let base = (
                NativeComponentKey::Base,
                io.take_native_custody(output.authority_verifier.logical_owned_bytes()?)?,
                io.take_native_custody(0)?,
            );
            let mut data = io.take_native_custody(retained - base.1.bytes())?;
            quota.transfer();
            let mut custody = NativeComponentCustody::default();
            drop(custody.install(Some(base), quota.work())?);
            if let Err(error) =
                NativeComponentCustody::from_reducer(&output, &mut data, &mut custody, quota.work())
            {
                drop(output);
                drop(custody);
                drop(data);
                return Err(error);
            }
            output.native_custody = custody;
        }
        quota.transfer();
        Ok(output)
    }
}
