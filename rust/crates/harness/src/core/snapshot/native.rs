//! Native checkpoint ownership and bounded decoding.
use super::*;
impl Snapshot {
    pub(super) fn require_work(&self, work: &Option<&mut dyn HistoryIo>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_allocation.is_some() && !work.as_deref().is_some_and(copying::is_native) {
            return Err(crate::fork::boundary_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "native snapshot requires explicit native work",
            ));
        }
        let _ = work;
        Ok(())
    }
    /// Pure capacity witness for splitting a predecoded outer DTO's original guard.
    pub(super) fn fields_owned_bytes(&self) -> Result<u64> {
        let mut bytes = owned_vector(&self.projection.bindings, |(extension, _)| {
            Ok(extension.name.capacity() as u64)
        })?;
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
        Ok(bytes)
    }
    /// Adopts a genuine predecoded payload claim without issuing native work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn adopt_predecoded_native_custody(
        mut self,
        allocation: acyclic_fs::NativeAllocation,
    ) -> Result<Self> {
        let incoming = allocation;
        if self.native_allocation.is_some() {
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: Some(incoming),
            });
        }
        self.native_allocation = Some(incoming);
        let required = self.predecoded_native_owned_bytes()?;
        if self
            .native_allocation
            .as_ref()
            .expect("installed predecoded custody")
            .bytes()
            < required
        {
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: self.native_allocation.take(),
            });
        }
        Ok(self)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn predecoded_native_owned_bytes(&self) -> Result<u64> {
        let conversation = &self.projection.conversation;
        ownership::owned_sum([
            self.fields_owned_bytes()?,
            conversation.native_unclaimed_message_buffer_bytes()?,
            conversation.native_unclaimed_archived_prefix_bytes()?,
            conversation.native_unclaimed_cache_bytes()?,
            conversation
                .messages()
                .iter()
                .try_fold(0, |bytes, message| {
                    ownership::owned_sum([bytes, ownership::owned_message(message)?])
                })?,
        ])
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn install_native_custody(
        &mut self,
        allocation: acyclic_fs::NativeAllocation,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.native_allocation = Some(allocation);
        self.adopt_unclaimed_native_conversation_custody(work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn adopt_unclaimed_native_conversation_custody(
        &mut self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let bytes = self
            .projection
            .conversation
            .native_unclaimed_message_buffer_bytes()?;
        if bytes != 0 {
            let shell = self
                .native_allocation
                .as_mut()
                .expect("installed snapshot custody")
                .split_off(bytes)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            self.projection
                .conversation
                .install_native_message_buffer(shell);
        }
        let archive_bytes = self
            .projection
            .conversation
            .native_unclaimed_archived_prefix_bytes()?;
        if archive_bytes != 0 {
            let archive = self
                .native_allocation
                .as_mut()
                .expect("installed snapshot custody")
                .split_off(archive_bytes)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            self.projection
                .conversation
                .install_native_archived_prefix(archive);
        }
        let cache_bytes = self
            .projection
            .conversation
            .native_unclaimed_cache_bytes()?;
        if cache_bytes != 0 {
            let cache = self
                .native_allocation
                .as_mut()
                .expect("installed snapshot custody")
                .split_off(cache_bytes)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            self.projection
                .conversation
                .install_native_preclaimed_caches(cache, work)?;
        }
        Ok(())
    }

    /// Decodes one admitted checkpoint and its existing conversation cache.
    pub fn decode(
        bytes: &[u8],
        maximum_bytes: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut admitted = 0;
        let admission = crate::executor::json::snapshot_json_admission(
            bytes,
            crate::executor::json::JsonBounds {
                maximum_bytes,
                maximum_items: maximum_bytes,
                maximum_depth: 256,
            },
            &mut |charge| {
                if let crate::executor::json::JsonCodecCharge::Admission {
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
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                native_allocation: _,
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
        let mut snapshot = Snapshot {
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
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_allocation: None,
        };
        if let Some(io) = quota.work().as_deref() {
            io.check_cancellation()?;
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if quota.work().as_deref().is_some_and(copying::is_native) {
            quota.replace(snapshot.predecoded_native_owned_bytes()?)?;
            let bytes = quota.retained();
            let allocation = quota
                .work()
                .as_deref_mut()
                .expect("native decoder observer")
                .take_native_custody(bytes)?;
            quota.transfer();
            snapshot.install_native_custody(allocation, quota.work())?;
        }
        quota.transfer();
        Ok(snapshot)
    }
}
