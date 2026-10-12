//! Restoring authenticated fields and retiring conversation prefixes.
use super::*;
impl Reducer {
    /// Restores authenticated state without replaying transitions.
    pub fn restore(snapshot: Snapshot, verifier: AuthorityVerifier, schemas: SchemaRegistry) -> Result<Self> {
        Self::restore_with_work(snapshot, verifier, schemas, &mut None)
    }
    /// Restores under the supplied native metadata ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn restore_native(snapshot: Snapshot, verifier: AuthorityVerifier, schemas: SchemaRegistry,
        native: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        let mut io = crate::store::history::native_work_io(native).map_err(Error::from)?;
        let bytes = verifier.logical_owned_bytes()?;
        io.admit_owned(bytes)?;
        io.retain_owned(bytes)?;
        Self::restore_with_work(snapshot, verifier, schemas, &mut Some(&mut io))
    }
    pub(crate) fn restore_with_work(mut snapshot: Snapshot, authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        snapshot.require_work(work)?;
        snapshot.verify(&authority_verifier, &schemas, work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = work.as_deref().is_some_and(copying::is_native);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if native && snapshot.native_allocation.is_none() {
            let bytes = snapshot.predecoded_native_owned_bytes()?;
            let io = work.as_deref_mut().expect("native restoration observer");
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
            let allocation = io.take_native_custody(bytes)?;
            snapshot.install_native_custody(allocation, work)?;
        }
        let mut custody = NativeComponentCustody::default();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut metadata = if native {
            snapshot.adopt_unclaimed_native_conversation_custody(work)?;
            let bytes = ownership::owned_sum([
                ownership::owned_buffer::<(ExtensionDependency, ExtensionConfiguration)>(snapshot.projection.configured_extensions.len())?,
                ownership::owned_buffer::<(Authority, ForkSeed)>(snapshot.projection.forks.len())?,
            ])?;
            let io = work.as_deref_mut().expect("native restoration observer");
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
            Some(io.take_native_custody(bytes)?)
        } else { None };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if native {
            let io = work.as_deref_mut().expect("native verifier observer");
            let base = (NativeComponentKey::Base, io.take_native_custody(authority_verifier.logical_owned_bytes()?)?, io.take_native_custody(0)?);
            drop(custody.install(Some(base), work)?);
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut allocation = snapshot.native_allocation.take();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _retired_vectors = if let Some(allocation) = allocation.as_mut() {
            let bytes = ownership::owned_sum([
                ownership::owned_buffer::<(ExtensionDependency, ExtensionConfiguration)>(snapshot.projection.configured_extensions.capacity())?,
                ownership::owned_buffer::<(Authority, ForkSeed)>(snapshot.projection.forks.capacity())?,
            ])?;
            Some(allocation.split_off(bytes).map_err(|cause| Error::NativeWork { cause, custody: None })?)
        } else { None };
        let projection = snapshot.projection;
        let mut reducer = Self {
            authority: snapshot.authority, authority_verifier, schemas,
            revision: snapshot.revision, lifecycle: projection.lifecycle,
            active_extensions: projection.active_extensions, active_extension_revision: projection.active_extension_revision,
            extension_states: projection.extension_states,
            configured_extensions: projection.configured_extensions.into_iter().collect(),
            active_configurations: projection.active_configurations, events: snapshot.events.into(),
            resident_event_limit: usize::MAX, effects: projection.effects,
            indexed_terminal_effects: projection.indexed_terminal_effects, effects_archived: projection.effects_archived,
            resident_terminal_effect_limit: usize::MAX, forks: projection.forks.into_iter().collect(),
            published_merges: projection.published_merges, conversation: projection.conversation,
            latest_context_checkpoint: projection.latest_context_checkpoint, interactions: projection.interactions,
            native_custody: NativeComponentCustody::default(),
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(allocation) = allocation.as_mut() {
            NativeComponentCustody::from_reducer(&reducer, allocation, metadata.as_mut().expect("native map metadata"), &mut custody, work)?;
        }
        reducer.native_custody = custody;
        Ok(reducer)
    }
    pub(crate) fn compact_conversation_projection(&mut self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        let Some(selection) = self.resident_checkpoint_selection() else { return Ok(()); };
        if selection.conversation_revision <= self.conversation.resident_after_sequence()
            || self.conversation.unresolved_user().is_some() { return Ok(()); }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(copying::is_native) {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let mut copied = copying::reserve_copy_vector::<(u64, Option<native_custody::Component>)>(
                self.conversation.messages().len(), &mut quota)?;
            for message in self.conversation.messages() { copied.push((message.sequence, None)); }
            let mut position = 0;
            let (replacement, _) = self.conversation.checkpointed_suffix(selection, quota.work(), Some(&mut |sequence, produced, work| {
                while position < copied.len() && copied[position].0 < sequence { position += 1; }
                if position == copied.len() || copied[position].0 != sequence {
                    return Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Invalid, "conversation component identity is invalid"));
                }
                let io = work.as_deref_mut().expect("native retirement observer");
                copied[position].1 = Some((NativeComponentKey::ConversationMessage(sequence), io.take_native_custody(produced)?, io.take_native_custody(0)?));
                position += 1;
                Ok(())
            }))?;
            self.conversation = replacement;
            for (sequence, claim) in copied {
                if let Some(claim) = claim { drop(self.native_custody.install(Some(claim), quota.work())?); }
                else { self.native_custody.remove(&NativeComponentKey::ConversationMessage(sequence)); }
            }
            return Ok(());
        }
        self.conversation = self.conversation.checkpointed_suffix(selection, work, None)?.0;
        Ok(())
    }
}
