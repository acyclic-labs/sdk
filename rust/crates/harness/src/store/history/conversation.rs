use super::*;

impl<P: StreamProvider> HistoryReader<P> {
    /// Captures the latest canonical message using one head locator and one owner-attested event.
    pub async fn latest_conversation_message(
        &self,
        maximum_bytes: u64,
    ) -> Result<Option<crate::conversation::ConversationMessage>> {
        crate::store::operations::latest_message(
            &self.client,
            self.verifier.audience(),
            &self.verifier,
            maximum_bytes,
        )
        .await
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
        self.verifier
            .verify_audience(&cursor.authority, &mut None)?;
        if limits.maximum_events == 0
            || limits.maximum_bytes == 0
            || selection.message_ids.len() as u64 > u64::from(limits.maximum_events)
            || cursor.after_revision > cursor.through_revision
        {
            return Err(Error::Invalid(
                "selected history bounds or cursor are invalid".into(),
            ));
        }
        if cursor.through_revision > self.committed_tail_with_work(&mut None).await? {
            return Err(Error::Invalid(
                "selected history exceeds committed cutoff".into(),
            ));
        }
        let mut consumed = 0_u64;
        let mut messages = Vec::with_capacity(selection.message_ids.len());
        for id in &selection.message_ids {
            let remaining = limits
                .maximum_bytes
                .checked_sub(consumed)
                .filter(|remaining| *remaining > 0)
                .ok_or_else(|| Error::Invalid("selected history exceeds byte bound".into()))?;
            let (message, bytes) = crate::store::operations::find_message_with_work(
                &self.client,
                &cursor.authority,
                &self.verifier,
                *id,
                cursor.through_revision,
                remaining,
                &mut None,
            )
            .await?;
            consumed = consumed
                .checked_add(bytes)
                .ok_or_else(|| Error::Invalid("selected history byte count overflows".into()))?;
            messages.push(message.ok_or_else(|| {
                Error::Storage("selected history message is missing at its pinned cutoff".into())
            })?);
        }
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
        self.verifier
            .verify_audience(&cursor.authority, &mut None)?;
        if limits.maximum_events == 0
            || limits.maximum_bytes == 0
            || after_sequence > through_sequence
            || through_sequence - after_sequence > u64::from(limits.maximum_events)
            || cursor.after_revision > cursor.through_revision
        {
            return Err(Error::Invalid(
                "conversation range bounds or cursor are invalid".into(),
            ));
        }
        if cursor.through_revision > self.committed_tail_with_work(work).await? {
            return Err(Error::Invalid(
                "conversation range exceeds committed event cutoff".into(),
            ));
        }
        let mut messages = Vec::new();
        let mut consumed = 0_u64;
        if after_sequence == through_sequence {
            return Ok(messages);
        }
        for sequence in after_sequence + 1..=through_sequence {
            let remaining = limits
                .maximum_bytes
                .checked_sub(consumed)
                .filter(|bytes| *bytes > 0)
                .ok_or_else(|| Error::Invalid("conversation range exceeds byte bound".into()))?;
            let (message, bytes) = crate::store::operations::find_message_sequence_with_work(
                &self.client,
                &cursor.authority,
                &self.verifier,
                sequence,
                cursor.through_revision,
                remaining,
                work,
            )
            .await?;
            consumed = consumed
                .checked_add(bytes)
                .ok_or_else(|| Error::Invalid("conversation range byte count overflows".into()))?;
            if let Some(work) = work.as_deref_mut() {
                work.reserve_messages(&mut messages, limits.maximum_events as usize)?;
            }
            messages.push(message.ok_or_else(|| {
                Error::Storage("conversation range is incomplete at its pinned cutoff".into())
            })?);
        }
        Ok(messages)
    }
}
