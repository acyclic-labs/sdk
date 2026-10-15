use super::*;

pub(super) type RetainMessage<'callback, 'work> = dyn FnMut(u64, u64, &mut Option<&'work mut dyn crate::store::history::HistoryIo>) -> Result<()>
    + 'callback;

impl ConversationState {
    pub(super) fn require_native_work(
        &self,
        work: &Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if (self.native_custody.messages.is_some()
            || self.native_custody.digests.is_some()
            || self.native_custody.archive.is_some()
            || self.native_custody.index_buffer.is_some()
            || !self.native_custody.indexes.is_empty())
            && !work
                .as_deref()
                .is_some_and(|io| io.original_native_work().is_some())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native conversation ownership requires explicit native work",
            });
        }
        let _ = work;
        Ok(())
    }

    pub(crate) fn copy<'work>(
        &self,
        work: &mut Option<&'work mut dyn crate::store::history::HistoryIo>,
        mut retain: Option<&mut super::clone::RetainMessage<'_, 'work>>,
    ) -> Result<(Self, u64)> {
        if retain.is_some() {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if !work
                .as_deref()
                .is_some_and(|io| io.original_native_work().is_some())
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "message component custody requires original native work",
                });
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            return Err(Error::Invalid(
                "message component custody requires original native work".into(),
            ));
        }
        self.require_native_work(work)?;
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut state = Self {
            agent: self.agent,
            pending_user: self.pending_user,
            ..Self::default()
        };
        let retained = reserve_conversation_buffer(
            &mut state.messages,
            self.messages.len(),
            cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut state.native_custody.messages,
        )?;
        cache.adopt(retained)?;
        let retained = reserve_conversation_buffer(
            &mut state.prefix_digests,
            self.prefix_digests.len(),
            cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut state.native_custody.digests,
        )?;
        cache.adopt(retained)?;
        for _ in 0..self.by_id.len() {
            let retained = state.native_custody.retain_index(cache.work())?;
            cache.adopt(retained)?;
        }
        for message in &self.messages {
            let before = cache.retained();
            let message = message.clone_in_quota(&mut cache)?;
            if let Some(retain) = retain.as_mut() {
                let produced = cache.retained() - before;
                retain(message.sequence, produced, cache.work())?;
                cache.transfer();
            }
            state.messages.push(message);
        }
        let bytes =
            conversation_buffer_bytes::<[u8; 32]>(self.prefix_digests.len()).map_err(|reason| {
                conversation_error(cache.work(), crate::wire::ErrorCode::Invalid, reason)
            })?;
        if let Some(io) = cache.work().as_deref_mut() {
            io.admit_copy(bytes)?;
            io.copy_bytes(bytes)?;
        }
        state.prefix_digests.extend_from_slice(&self.prefix_digests);
        for (id, position) in &self.by_id {
            state.by_id.insert(*id, *position);
        }
        if let Some(prefix) = &self.archived_prefix {
            let retained = state.copy_archive_checkpoint(
                prefix.through_sequence,
                prefix.history_digest,
                &prefix.checkpoint,
                cache.work(),
            )?;
            cache.adopt(retained)?;
        }
        let retained = cache.retained();
        cache.transfer();
        Ok((state, retained))
    }

    pub(super) fn copy_archive_checkpoint(
        &mut self,
        through_sequence: u64,
        history_digest: [u8; 32],
        checkpoint: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let checkpoint = checkpoint.clone_in_quota(&mut quota)?;
        let bytes = quota.retained();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let retained = if let Some(io) = quota
            .work()
            .as_deref_mut()
            .filter(|io| io.original_native_work().is_some())
        {
            self.native_custody.archive = Some(io.take_native_custody(bytes)?);
            0
        } else {
            bytes
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let retained = bytes;
        quota.transfer();
        self.archived_prefix = Some(ArchivedConversationPrefix {
            through_sequence,
            history_digest,
            checkpoint,
        });
        Ok(retained)
    }
}
