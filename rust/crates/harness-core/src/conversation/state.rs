use super::*;

impl ConversationState {
    pub(crate) fn from_wire(
        wire: ConversationStateWire,
        maximum_encoded_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        Self {
            agent: wire.agent,
            messages: wire.messages,
            archived_prefix: wire.archived_prefix,
            ..Self::default()
        }
        .rebuild(maximum_encoded_bytes, work)
    }

    pub(super) fn rebuild(
        self,
        maximum_encoded_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut state = self;
        if let Some(prefix) = &state.archived_prefix {
            prefix.checkpoint.validate(cache.work())?;
            if state.agent.is_none() || prefix.through_sequence == 0 {
                return Err(conversation_error(
                    cache.work(),
                    crate::wire::ErrorCode::Invalid,
                    "archived conversation prefix is unbound",
                ));
            }
        }
        let mut previous = state
            .archived_prefix
            .as_ref()
            .map_or(0, |prefix| prefix.through_sequence);
        let retained = reserve_conversation_buffer(
            &mut state.prefix_digests,
            state.messages.len(),
            cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut state.native_custody.digests,
        )?;
        cache.adopt(retained)?;
        for (position, message) in state.messages.iter().enumerate() {
            if let Some(io) = cache.work().as_deref_mut() {
                io.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                    bytes: 0,
                    items: 1,
                })?;
            }
            message.validate(cache.work())?;
            if message.sequence <= previous
                || (state.archived_prefix.is_some()
                    && previous.checked_add(1) != Some(message.sequence))
                || state.by_id.contains_key(&message.id)
            {
                return Err(conversation_error(
                    cache.work(),
                    crate::wire::ErrorCode::Invalid,
                    "conversation order or identity is invalid",
                ));
            }
            let retained = state.native_custody.retain_index(cache.work())?;
            cache.adopt(retained)?;
            state.by_id.insert(message.id, (Some(position), None));
            previous = message.sequence;
            let digest = advance_history_digest(
                state.history_digest(),
                message,
                maximum_encoded_bytes,
                cache.work(),
            )?;
            state.prefix_digests.push(digest);
            if message.kind == MessageKind::User {
                state.pending_user = Some(message.id);
            }
        }
        for (position, message) in state.messages.iter().enumerate() {
            if let Some(io) = cache.work().as_deref_mut() {
                io.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                    bytes: 0,
                    items: 1,
                })?;
            }
            if let Some(parent) = turn_outcome_parent(message)
                && !state.by_id.contains_key(&parent)
            {
                let retained = state.native_custody.retain_index(cache.work())?;
                cache.adopt(retained)?;
            }
            settle_turn(&mut state.pending_user, &mut state.by_id, message, position);
        }
        let retained = cache.retained();
        cache.transfer();
        Ok((state, retained))
    }
}
