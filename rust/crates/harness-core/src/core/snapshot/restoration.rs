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
            projection: snapshot.projection.map(Ok, |_| ())?,
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
        let mut quota = CodecQuota::new(work, 0)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = quota.work().as_deref().is_some_and(copying::is_native);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let native = false;
        let mut admitted = 0;
        let decoded = crate::executor::json::json_from_slice_bounded(
            std::marker::PhantomData::<Snapshot<crate::conversation::ConversationStateWire>>,
            bytes,
            crate::executor::json::JsonBounds {
                maximum_bytes,
                maximum_items: maximum_bytes,
                maximum_depth: 256,
            },
            native.then_some(crate::wire::ErrorCode::Storage),
            Some(crate::executor::json::JsonValueSchema::Snapshot),
            &mut |charge| {
                use crate::executor::json::JsonCodecCharge::Admission;
                let (charge, reserved) = match charge {
                    Admission {
                        maximum_owned_bytes,
                        value_shape,
                    } => {
                        let diagnostics =
                            crate::executor::json::history_diagnostic_bytes(value_shape, None)?;
                        let bytes =
                            maximum_owned_bytes
                                .checked_add(diagnostics)
                                .ok_or_else(|| {
                                    Error::Storage("checkpoint admission overflows".into())
                                })?;
                        let charge = Admission {
                            maximum_owned_bytes: bytes,
                            value_shape,
                        };
                        (charge, bytes)
                    }
                    charge => (charge, 0),
                };
                if let Some(io) = quota.work().as_deref_mut() {
                    io.codec_charge(charge)?;
                }
                admitted += reserved;
                Ok(())
            },
        );
        quota.adopt(admitted)?;
        let wire = decoded.map_err(|error| match quota.json_error(error) {
            Error::Invalid(reason) => Error::Storage(reason),
            error => error,
        })?;
        let Snapshot {
            format_version,
            authority,
            revision,
            events,
            projection,
            state_digest,
            attestation,
        } = wire;
        let projection = projection.map(
            |conversation| {
                let (conversation, retained) =
                    ConversationState::from_wire(conversation, maximum_bytes, quota.work())?;
                quota.adopt(retained)?;
                Ok(conversation)
            },
            |bindings| bindings,
        )?;
        let snapshot = Snapshot {
            format_version,
            authority,
            revision,
            events,
            state_digest,
            attestation,
            projection,
        };
        if let Some(io) = quota.work().as_deref() {
            io.check_cancellation()?;
        }
        quota.transfer();
        Ok(snapshot)
    }
}
