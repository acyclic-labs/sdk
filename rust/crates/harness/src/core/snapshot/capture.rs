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
        inspect_metadata(self, work)?;
        let bytes = if work.is_some() {
            self.fields_owned_bytes()?
        } else {
            0
        };
        let mut quota = admit_copy(bytes, work)?;
        let (conversation, retained) = self.projection.conversation.copy(quota.work(), None)?;
        quota.adopt(retained)?;
        let snapshot = Self {
            format_version: self.format_version,
            authority: self.authority.clone(),
            revision: self.revision,
            events: self.events.clone(),
            projection: self
                .projection
                .copy(conversation, self.projection.bindings.clone()),
            state_digest: self.state_digest,
            attestation: self.attestation,
        };
        quota.transfer();
        Ok(snapshot)
    }
    fn fields_owned_bytes(&self) -> Result<u64> {
        ownership::owned_sum([
            owned_vector(&self.projection.bindings, |(extension, _)| {
                Ok(extension.name.capacity() as u64)
            })?,
            self.projection.owned_bytes(
                &self.authority,
                self.events.iter(),
                self.events.capacity(),
            )?,
        ])
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
        if maximum_events == 0 && self.revision != 0 {
            return Err(Error::Invalid("snapshot must retain its head event".into()));
        }
        let bytes = if work.is_some() {
            self.projection.owned_bytes(
                &self.authority,
                self.events.iter(),
                self.events.capacity(),
            )?
        } else {
            0
        };
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
            let digest = crate::executor::json::canonical_json_digest(
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
