use super::*;

impl<P: StreamProvider> HistoryReader<P> {
    /// Captures the latest canonical message using one head locator and one owner-attested event.
    pub async fn latest_conversation_message(
        &self,
        maximum_bytes: u64,
    ) -> Result<Option<crate::conversation::ConversationMessage>> {
        crate::store::operations::find_message_with_work(
            &self.client,
            self.verifier.audience(),
            &self.verifier,
            crate::store::operations::MessageKey::Head,
            u64::MAX,
            maximum_bytes,
            &mut None,
        )
        .await
        .map(|(message, _)| message)
    }

    /// Resolves one message at the pinned boundary using its atomic locator.
    pub async fn conversation_message(
        &self,
        cursor: &HistoryCursor,
        message_id: uuid::Uuid,
        maximum_bytes: u64,
    ) -> Result<Option<crate::conversation::ConversationMessage>> {
        self.verifier
            .verify_audience(&cursor.authority, &mut None)?;
        if cursor.after_revision > cursor.through_revision
            || cursor.through_revision > self.committed_tail_with_work(&mut None).await?
        {
            return Err(Error::Invalid("message lookup cursor is invalid".into()));
        }
        crate::store::operations::find_message_with_work(
            &self.client,
            &cursor.authority,
            &self.verifier,
            message_id,
            cursor.through_revision,
            maximum_bytes,
            &mut None,
        )
        .await
        .map(|(message, _)| message)
    }

    /// Resolves only the declared selection through authenticated atomic indexes.
    pub async fn selected_conversation(
        &self,
        cursor: &HistoryCursor,
        selection: &crate::conversation::ModelContextSelection,
        limits: HistoryReadLimits,
    ) -> Result<crate::conversation::ConversationState> {
        if selection.message_ids.len() as u64 > u64::from(limits.maximum_events) {
            return Err(Error::Invalid(
                "selected history exceeds event bound".into(),
            ));
        }
        let messages = self
            .indexed_messages(
                cursor,
                selection
                    .message_ids
                    .iter()
                    .copied()
                    .map(crate::store::operations::MessageKey::Id),
                limits,
                &mut None,
            )
            .await?;
        let view = crate::conversation::ConversationState::from_wire(
            crate::conversation::ConversationStateWire {
                agent: None,
                messages,
                archived_prefix: None,
            },
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut None,
        )
        .map(|(state, _)| state)?;
        crate::projection::validate_model_context_selection(
            &view,
            selection,
            selection.conversation_revision,
            &mut None,
        )?;
        Ok(view)
    }

    /// Reads an exact canonical message range at one event cutoff.
    pub async fn conversation_range(
        &self,
        cursor: &HistoryCursor,
        after_sequence: u64,
        through_sequence: u64,
        limits: HistoryReadLimits,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Vec<crate::conversation::ConversationMessage>> {
        if after_sequence > through_sequence
            || through_sequence - after_sequence > u64::from(limits.maximum_events)
        {
            return Err(Error::Invalid(
                "conversation range bounds are invalid".into(),
            ));
        }
        self.indexed_messages(
            cursor,
            (after_sequence..through_sequence)
                .map(|sequence| crate::store::operations::MessageKey::Sequence(sequence + 1)),
            limits,
            work,
        )
        .await
    }

    async fn indexed_messages(
        &self,
        cursor: &HistoryCursor,
        keys: impl Iterator<Item = crate::store::operations::MessageKey>,
        limits: HistoryReadLimits,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Vec<crate::conversation::ConversationMessage>> {
        self.verifier
            .verify_audience(&cursor.authority, &mut None)?;
        if limits.maximum_events == 0
            || limits.maximum_bytes == 0
            || cursor.after_revision > cursor.through_revision
            || cursor.through_revision > self.committed_tail_with_work(work).await?
        {
            return Err(Error::Invalid(
                "indexed history bounds or cursor are invalid".into(),
            ));
        }
        let mut messages = Vec::new();
        let mut consumed = 0_u64;
        for key in keys {
            let remaining = limits
                .maximum_bytes
                .checked_sub(consumed)
                .filter(|remaining| *remaining > 0)
                .ok_or_else(|| Error::Invalid("indexed history exceeds byte bound".into()))?;
            let (message, bytes) = crate::store::operations::find_message_with_work(
                &self.client,
                &cursor.authority,
                &self.verifier,
                key,
                cursor.through_revision,
                remaining,
                work,
            )
            .await?;
            consumed = consumed
                .checked_add(bytes)
                .ok_or_else(|| Error::Invalid("indexed history byte count overflows".into()))?;
            if let Some(work) = work.as_deref_mut() {
                crate::store::history::ensure_slot(
                    &mut messages,
                    limits.maximum_events as usize,
                    work,
                )?;
            }
            messages.push(message.ok_or_else(|| {
                Error::Storage("indexed history is missing at its pinned cutoff".into())
            })?);
        }
        Ok(messages)
    }
}
