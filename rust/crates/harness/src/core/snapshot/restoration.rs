use super::*;
impl Reducer {
    /// Restores authenticated state without replaying transitions.
    pub fn restore(
        snapshot: Snapshot,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        snapshot.verify(&authority_verifier, &schemas, work)?;
        Ok(Self {
            authority: snapshot.authority,
            authority_verifier,
            schemas,
            revision: snapshot.revision,
            projection: snapshot.projection.with_bindings(()),
            events: snapshot.events.into(),
            resident_event_limit: usize::MAX,
            resident_terminal_effect_limit: usize::MAX,
        })
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
        self.projection.conversation = self
            .projection
            .conversation
            .checkpointed_suffix(selection, work, None)?
            .0;
        Ok(())
    }
}

impl Snapshot {
    /// Decodes one admitted checkpoint and its existing conversation cache.
    pub fn decode(
        bytes: &[u8],
        maximum_bytes: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut admitted = 0;
        let admission = crate::contract::snapshot_json_admission(
            bytes,
            crate::contract::JsonBounds {
                maximum_bytes,
                maximum_items: maximum_bytes,
                maximum_depth: 256,
            },
            &mut |charge| {
                if let crate::contract::JsonCodecCharge::Admission {
                    maximum_owned_bytes,
                    ..
                } = charge
                {
                    admitted = maximum_owned_bytes;
                }
                match quota.work().as_deref_mut() {
                    Some(io) => io.codec_charge(charge),
                    None => Ok(()),
                }
            },
        );
        quota.adopt(admitted)?;
        let (_, conversation_allowance) = admission?;
        let mut decoder = serde_json::Deserializer::from_slice(bytes);
        decoder.disable_recursion_limit();
        let decoded =
            Snapshot::<crate::conversation::ConversationStateWire>::deserialize(&mut decoder)
                .and_then(|value| decoder.end().map(|()| value));
        let wire = match decoded {
            Ok(snapshot) => snapshot,
            Err(source) => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if quota.work().as_deref().is_some_and(copying::is_native) {
                    let bytes = quota.retained();
                    let custody = quota
                        .work()
                        .as_deref_mut()
                        .expect("native decoder observer")
                        .take_native_custody(bytes)?;
                    quota.transfer();
                    return Err(Error::NativeJson {
                        code: crate::wire::ErrorCode::Storage,
                        source,
                        custody: Some(custody),
                    });
                }
                return Err(Error::Storage(source.to_string()));
            }
        };
        quota.relinquish(conversation_allowance)?;
        let Snapshot {
            format_version,
            authority,
            revision,
            events,
            projection,
            state_digest,
            attestation,
        } = wire;
        let Projection {
            conversation,
            lifecycle,
            active_extensions,
            active_extension_revision,
            extension_states,
            configured_extensions,
            active_configurations,
            effects,
            indexed_terminal_effects,
            effects_archived,
            forks,
            published_merges,
            latest_context_checkpoint,
            interactions,
            bindings,
        } = projection;
        let (conversation, retained) =
            ConversationState::from_wire(conversation, maximum_bytes, quota.work())?;
        quota.adopt(retained)?;
        let snapshot = Snapshot {
            format_version,
            authority,
            revision,
            events,
            state_digest,
            attestation,
            projection: Projection {
                conversation,
                lifecycle,
                active_extensions,
                active_extension_revision,
                extension_states,
                configured_extensions,
                active_configurations,
                effects,
                indexed_terminal_effects,
                effects_archived,
                forks,
                published_merges,
                latest_context_checkpoint,
                interactions,
                bindings,
            },
        };
        if let Some(io) = quota.work().as_deref() {
            io.check_cancellation()?;
        }
        quota.transfer();
        Ok(snapshot)
    }
}
