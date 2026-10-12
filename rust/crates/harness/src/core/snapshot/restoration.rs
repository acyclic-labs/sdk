//! Restoring authenticated fields and retiring conversation prefixes.
use super::*;
impl Reducer {
    /// Restores authenticated state without replaying transitions.
    pub fn restore(
        mut snapshot: Snapshot,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
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
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let base = if native {
            snapshot.adopt_unclaimed_native_conversation_custody(work)?;
            let io = work.as_deref_mut().expect("native verifier observer");
            Some((
                NativeComponentKey::Base,
                io.take_native_custody(authority_verifier.logical_owned_bytes()?)?,
                io.take_native_custody(0)?,
            ))
        } else {
            None
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut allocation = snapshot.native_allocation.take();
        let projection = snapshot.projection;
        let mut reducer = Self {
            authority: snapshot.authority,
            authority_verifier,
            schemas,
            revision: snapshot.revision,
            projection: projection.with_bindings(()),
            events: snapshot.events.into(),
            resident_event_limit: usize::MAX,
            resident_terminal_effect_limit: usize::MAX,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_components: Vec::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_component_buffer: None,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            drop(reducer.install_component(base, work)?);
            if let Some(allocation) = allocation.as_mut() {
                reducer.adopt_projection(allocation, work)?;
            }
        }
        Ok(reducer)
    }
    pub(crate) fn compact_conversation_projection(
        &mut self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let Some(selection) = self.resident_checkpoint_selection() else {
            return Ok(());
        };
        if selection.conversation_revision <= self.projection.conversation.resident_after_sequence()
            || self.projection.conversation.unresolved_user().is_some()
        {
            return Ok(());
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(copying::is_native) {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let mut copied = copying::reserve_copy_vector::<(u64, Option<reducer::Component>)>(
                self.projection.conversation.messages().len(),
                &mut quota,
            )?;
            for message in self.projection.conversation.messages() {
                copied.push((message.sequence, None));
            }
            let mut position = 0;
            let (replacement, _) = self.projection.conversation.checkpointed_suffix(
                selection,
                quota.work(),
                Some(&mut |sequence, produced, work| {
                    while position < copied.len() && copied[position].0 < sequence {
                        position += 1;
                    }
                    if position == copied.len() || copied[position].0 != sequence {
                        return Err(crate::fork::boundary_error(
                            work,
                            crate::wire::ErrorCode::Invalid,
                            "conversation component identity is invalid",
                        ));
                    }
                    let io = work.as_deref_mut().expect("native retirement observer");
                    copied[position].1 = Some((
                        NativeComponentKey::ConversationMessage(sequence),
                        io.take_native_custody(produced)?,
                        io.take_native_custody(0)?,
                    ));
                    position += 1;
                    Ok(())
                }),
            )?;
            self.projection.conversation = replacement;
            for (sequence, claim) in copied {
                if let Some(claim) = claim {
                    drop(self.install_component(Some(claim), quota.work())?);
                } else {
                    self.remove_component(&NativeComponentKey::ConversationMessage(sequence));
                }
            }
            return Ok(());
        }
        self.projection.conversation = self
            .projection
            .conversation
            .checkpointed_suffix(selection, work, None)?
            .0;
        Ok(())
    }
}
