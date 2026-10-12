//! Admission and capture of bounded checkpoint data.
use super::*;

fn admit_copy<'a, 'w>(
    bytes: u64,
    work: &'a mut Option<&'w mut dyn HistoryIo>,
) -> Result<crate::wire_codec::CodecQuota<'a, 'w>> {
    if let Some(io) = work.as_deref() {
        io.admit_copy(bytes)?;
    }
    let mut quota = crate::wire_codec::CodecQuota::new(work, bytes)?;
    if let Some(io) = quota.work().as_deref_mut() {
        io.copy_bytes(bytes)?;
    }
    Ok(quota)
}
impl Snapshot {
    /// Copies authenticated state under the optional work ledger.
    pub fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        self.require_work(work)?;
        source_metadata_inspection_with_work(self, work)?;
        let bytes = if work.is_some() {
            self.fields_owned_bytes()?
        } else {
            0
        };
        let mut quota = admit_copy(bytes, work)?;
        let (conversation, retained) = self.projection.conversation.copy(quota.work(), None)?;
        quota.adopt(retained)?;
        let mut snapshot = Self {
            format_version: self.format_version,
            authority: self.authority.clone(),
            revision: self.revision,
            events: self.events.clone(),
            projection: self
                .projection
                .copy(conversation, self.projection.bindings.clone()),
            state_digest: self.state_digest,
            attestation: self.attestation,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_allocation: None,
        };
        snapshot.finish(&mut quota)?;
        quota.transfer();
        Ok(snapshot)
    }
    fn finish(&mut self, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if quota.work().as_deref().is_some_and(copying::is_native) {
            let bytes = quota.retained();
            let allocation = quota
                .work()
                .as_deref_mut()
                .expect("native snapshot observer")
                .take_native_custody(bytes)?;
            quota.transfer();
            self.install_native_custody(allocation, quota.work())?;
        }
        let _ = quota;
        Ok(())
    }
}
impl Reducer {
    /// Captures the projection after one committed event.
    pub(crate) fn publication_snapshot_with_work(
        &self,
        event: &Event,
        maximum_events: usize,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Snapshot, u64)> {
        let mut preview = self.copy(work)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let input = copying::copy_event(event, &mut quota)?;
        preview.apply_committed(input, quota.work())?;
        preview.mark_indexed_terminal_effect_with_work(event, quota.work())?;
        let result = preview.snapshot(maximum_events, quota.work());
        drop(preview);
        result
    }
    /// Captures authenticated state with a bounded event suffix.
    pub fn snapshot(
        &self,
        maximum_events: usize,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Snapshot, u64)> {
        self.native_custody.require_work(
            work,
            "native snapshot capture requires explicit native work",
        )?;
        if maximum_events == 0 && self.revision != 0 {
            return Err(Error::Invalid("snapshot must retain its head event".into()));
        }
        let mut bytes = 0;
        if work.is_some() {
            self.projection.visit(
                &self.authority,
                self.events.iter(),
                self.events.capacity(),
                |key, owned| {
                    if !matches!(key, NativeComponentKey::ConversationMessage(_)) {
                        bytes = ownership::owned_sum([bytes, owned])?;
                    }
                    Ok(())
                },
            )?;
        }
        let mut quota = admit_copy(bytes, work)?;
        let (conversation, retained) = match self.resident_checkpoint_selection() {
            Some(selection) => {
                self.projection
                    .conversation
                    .checkpointed_suffix(selection, quota.work(), None)?
            }
            None => self.projection.conversation.copy(quota.work(), None)?,
        };
        quota.adopt(retained)?;
        let schemas = self.schemas.schemas.as_deref();
        let count = schemas.map_or(0, BTreeMap::len);
        let binding_bytes = schemas.into_iter().flat_map(BTreeMap::iter).try_fold(
            ownership::owned_buffer::<(ExtensionDependency, [u8; 32])>(count)?,
            |bytes, ((name, _), _)| ownership::owned_sum([bytes, name.capacity() as u64]),
        )?;
        quota.replace(ownership::owned_sum([quota.retained(), binding_bytes])?)?;
        let mut bindings = Vec::with_capacity(count);
        for ((name, version), binding) in schemas.into_iter().flat_map(BTreeMap::iter) {
            let digest = crate::contract::canonical_json_digest_with_work(
                binding,
                crate::store::default_projection_read_limits().maximum_bytes,
                quota.work(),
            )?;
            if let Some(io) = quota.work().as_deref_mut() {
                io.copy_bytes(name.len() as u64)?;
            }
            bindings.push((
                ExtensionDependency {
                    name: name.clone(),
                    version: *version,
                },
                digest,
            ));
        }
        let mut snapshot = Snapshot {
            format_version: 6,
            authority: self.authority.clone(),
            revision: self.revision,
            events: self
                .events
                .iter()
                .skip(self.events.len().saturating_sub(maximum_events))
                .cloned()
                .collect(),
            projection: self.projection.copy(conversation, bindings),
            state_digest: [0; 32],
            attestation: [0; 32],
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_allocation: None,
        };
        snapshot.state_digest = snapshot.digest_with_work(quota.work())?;
        snapshot.attestation = self.authority_verifier.canonical_proof(
            b"harness/v6/reducer-checkpoint\0",
            &(
                &self.authority_verifier.id,
                &self.authority_verifier.audience,
                snapshot.state_digest,
            ),
            quota.work(),
        )?;
        snapshot.finish(&mut quota)?;
        let retained = quota.retained();
        quota.transfer();
        Ok((snapshot, retained))
    }
    pub(super) fn resident_checkpoint_selection(&self) -> Option<&ModelContextSelection> {
        self.events
            .iter()
            .rev()
            .find_map(|event| match &event.payload {
                EventPayload::ModelContextSelected { selection }
                    if selection.checkpoint.is_some()
                        && selection.checkpoint == self.projection.latest_context_checkpoint =>
                {
                    Some(selection)
                }
                _ => None,
            })
    }
}
