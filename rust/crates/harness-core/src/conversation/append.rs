use super::*;

impl ConversationState {
    /// Binds an empty conversation to exactly one agent.
    pub fn bind(&mut self, agent: AgentId) -> Result<()> {
        if self.agent.is_some() || !self.messages.is_empty() {
            return Err(Error::Conflict("conversation is already bound".into()));
        }
        self.agent = Some(agent);
        Ok(())
    }

    pub(super) fn adopt_existing_metadata(
        &mut self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work
            .as_deref_mut()
            .filter(|io| io.original_native_work().is_some())
        {
            adopt_existing_buffer::<ConversationMessage>(
                &mut self.native_custody.messages,
                self.messages.capacity(),
                io,
            )?;
            adopt_existing_buffer::<[u8; 32]>(
                &mut self.native_custody.digests,
                self.prefix_digests.capacity(),
                io,
            )?;
            if self.native_custody.archive.is_none()
                && let Some(prefix) = &self.archived_prefix
            {
                let bytes = prefix.checkpoint.logical_owned_bytes()?;
                io.admit_owned(bytes)?;
                io.retain_owned(bytes)?;
                self.native_custody.archive = Some(io.take_native_custody(bytes)?);
            }
            let mut native: Option<&mut dyn crate::store::history::HistoryIo> = Some(io);
            for _ in self.native_custody.indexes.len()..self.by_id.len() {
                self.native_custody.retain_index(&mut native)?;
            }
        }
        let _ = work;
        Ok(())
    }

    /// Appends one ordered, validated message.
    pub fn append(
        &mut self,
        message: ConversationMessage,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.require_native_work(work)?;
        self.validate_append(&message, work)?;
        let history_digest = advance_history_digest(
            self.history_digest(),
            &message,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        self.adopt_existing_metadata(work)?;
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        let retained = grow_conversation_buffer(
            &mut self.messages,
            cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut self.native_custody.messages,
        )?;
        cache.adopt(retained)?;
        let retained = grow_conversation_buffer(
            &mut self.prefix_digests,
            cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut self.native_custody.digests,
        )?;
        cache.adopt(retained)?;
        if !self.by_id.contains_key(&message.id) {
            let retained = self.native_custody.retain_index(cache.work())?;
            cache.adopt(retained)?;
        }
        self.by_id.entry(message.id).or_default().0 = Some(self.messages.len());
        if message.kind == MessageKind::User {
            self.pending_user = Some(message.id);
        }
        settle_turn(
            &mut self.pending_user,
            &mut self.by_id,
            &message,
            self.messages.len(),
        );
        self.messages.push(message);
        self.prefix_digests.push(history_digest);
        cache.transfer();
        Ok(())
    }

    pub(crate) fn validate_append(
        &self,
        message: &ConversationMessage,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.agent.ok_or_else(|| {
            conversation_error(
                work,
                crate::wire::ErrorCode::Conflict,
                "conversation is unbound",
            )
        })?;
        message.validate(work)?;
        if self.messages.last().is_some_and(|last| {
            last.sequence
                != self
                    .resident_after_sequence()
                    .saturating_add(self.messages.len() as u64)
        }) {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "sparse conversation history is read-only",
            ));
        }
        let expected = self.logical_revision().checked_add(1).ok_or_else(|| {
            conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "conversation sequence exhausted",
            )
        })?;
        if message.sequence != expected || self.message_position(message.id).is_some() {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Conflict,
                "message sequence or identity is invalid",
            ));
        }
        let parent = message
            .reply_to
            .map(|id| {
                self.message(id).ok_or_else(|| {
                    conversation_error(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "message reply target is missing",
                    )
                })
            })
            .transpose()?;
        if message.kind == MessageKind::ToolResult
            && !parent.is_some_and(|prior| {
                prior.kind == MessageKind::ToolCall
                    && prior.tool_call_id.as_deref() == message.tool_call_id.as_deref()
            })
        {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "tool result has no preceding call",
            ));
        }

        Ok(())
    }
}
