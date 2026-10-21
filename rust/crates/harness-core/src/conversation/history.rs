use super::*;

impl ConversationState {
    pub(crate) fn archived_prefix(&self) -> Option<&ArchivedConversationPrefix> {
        self.archived_prefix.as_ref()
    }
}

#[cfg(test)]
thread_local! {
    /// Negative-control seam: when set, a missing checkpoint pin is accepted as a no-op.
    pub(super) static PIN_GUARD_DISABLED: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

impl ConversationState {
    pub(super) fn history_digest(&self) -> [u8; 32] {
        self.prefix_digests.last().copied().unwrap_or_else(|| {
            self.archived_prefix
                .as_ref()
                .map_or([0; 32], |prefix| prefix.history_digest)
        })
    }

    /// Logical history head, independent of the number of resident records.
    #[must_use]
    pub fn logical_revision(&self) -> u64 {
        self.messages
            .last()
            .map_or(self.resident_after_sequence(), |message| message.sequence)
    }

    /// Canonical records through this cut are archived, not resident.
    #[must_use]
    pub fn resident_after_sequence(&self) -> u64 {
        self.archived_prefix
            .as_ref()
            .map_or(0, |prefix| prefix.through_sequence)
    }

    pub(crate) fn history_prefix_digest(
        &self,
        through_sequence: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        let base = self.resident_after_sequence();
        if self.logical_revision() != base.saturating_add(self.messages.len() as u64)
            || through_sequence < base
            || through_sequence > self.logical_revision()
        {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "history fingerprint requires a loaded dense logical prefix",
            ));
        }
        if through_sequence == base {
            return Ok(self
                .archived_prefix
                .as_ref()
                .map_or([0; 32], |prefix| prefix.history_digest));
        }
        let position = usize::try_from(through_sequence - base - 1).map_err(|_| {
            conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "history prefix exceeds platform bounds",
            )
        })?;
        self.prefix_digests.get(position).copied().ok_or_else(|| {
            conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "history prefix digest is missing",
            )
        })
    }

    pub(crate) fn checkpoint_covers_archived_prefix(
        &self,
        reference: &FileRef,
        through: u64,
    ) -> bool {
        self.archived_prefix.as_ref().is_some_and(|prefix| {
            prefix.checkpoint == *reference && prefix.through_sequence == through
        })
    }

    fn prefix_has_open_calls(
        &self,
        cut: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let mut temporary = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut calls = BTreeSet::new();
        for message in self
            .messages
            .iter()
            .take_while(|message| message.sequence <= cut)
        {
            if let Some(io) = temporary.work().as_deref_mut() {
                io.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                    bytes: 0,
                    items: 1,
                })?;
            }
            match message.kind {
                MessageKind::ToolCall => {
                    if !calls.contains(&message.id) {
                        let bytes = std::mem::size_of::<Uuid>() as u64;
                        if let Some(io) = temporary.work().as_deref_mut() {
                            io.admit_owned(bytes)?;
                            io.retain_owned(bytes)?;
                        }
                        temporary.adopt(bytes)?;
                        calls.insert(message.id);
                    }
                }
                MessageKind::ToolResult => {
                    if let Some(parent) = message.reply_to
                        && calls.remove(&parent)
                    {
                        temporary.relinquish(std::mem::size_of::<Uuid>() as u64)?;
                    }
                }
                _ => {}
            }
        }
        Ok(!calls.is_empty())
    }

    pub(crate) fn checkpointed_suffix<'work>(
        &self,
        selection: &ModelContextSelection,
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
        let Some(reference) = selection.checkpoint.as_ref() else {
            #[cfg(test)]
            if PIN_GUARD_DISABLED.get() {
                return self.copy(work, retain);
            }
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "history retirement requires a checkpoint pin",
            ));
        };
        let cut = selection.conversation_revision;
        if cut <= self.resident_after_sequence() || self.pending_user.is_some() {
            return self.copy(work, retain);
        }
        crate::projection::validate_model_context_selection(self, selection, cut, work)?;
        if self.prefix_has_open_calls(cut, work)? {
            return self.copy(work, retain);
        }
        let digest = self.history_prefix_digest(cut, work)?;
        let start = self
            .messages
            .partition_point(|message| message.sequence <= cut);
        let suffix = self.messages.iter().skip(start);
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut state = Self {
            agent: self.agent,
            ..Self::default()
        };
        let retained = reserve_conversation_buffer(
            &mut state.messages,
            suffix.len(),
            cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut state.native_custody.messages,
        )?;
        cache.adopt(retained)?;
        for message in suffix {
            let before = cache.retained();
            let message = message.clone_in_quota(&mut cache)?;
            if let Some(retain) = retain.as_mut() {
                let produced = cache.retained() - before;
                retain(message.sequence, produced, cache.work())?;
                cache.transfer();
            }
            state.messages.push(message);
        }
        let retained = state.copy_archive_checkpoint(cut, digest, reference, cache.work())?;
        cache.adopt(retained)?;
        let (state, retained) =
            state.rebuild(acyclic_stream::MAX_RECORD_BYTES as u64, cache.work())?;
        cache.adopt(retained)?;
        let retained = cache.retained();
        cache.transfer();
        Ok((state, retained))
    }
}
