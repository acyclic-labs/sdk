use super::*;

impl ConversationState {
    pub(crate) fn unresolved_user(&self) -> Option<Uuid> {
        self.pending_user
    }

    pub(crate) fn turn_outcome(&self, user: Uuid) -> Option<&ConversationMessage> {
        self.by_id
            .get(&user)
            .and_then(|(_, position)| *position)
            .and_then(|position| self.messages.get(position))
    }

    /// Loaded authoritative records, in sequence order. Mutation goes through `append`.
    #[must_use]
    pub fn messages(&self) -> &[ConversationMessage] {
        &self.messages
    }

    /// Reads a bounded archive page at a caller-pinned logical tail.
    pub fn page(&self, after: u64, through: u64, maximum: usize) -> Result<&[ConversationMessage]> {
        let tail = self.logical_revision();
        if maximum == 0
            || after < self.resident_after_sequence()
            || after > through
            || through > tail
        {
            return Err(Error::Invalid(
                "conversation page cursor or bound is invalid".into(),
            ));
        }
        let start = self
            .messages
            .partition_point(|message| message.sequence <= after);
        let end = self
            .messages
            .partition_point(|message| message.sequence <= through)
            .min(start.saturating_add(maximum));
        self.messages
            .get(start..end)
            .ok_or_else(|| Error::Invalid("conversation page range is invalid".into()))
    }

    /// Reads one resident message by identity.
    #[must_use]
    pub fn message(&self, id: Uuid) -> Option<&ConversationMessage> {
        self.message_position(id)
            .and_then(|position| self.messages.get(position))
    }

    pub(crate) fn message_position(&self, id: Uuid) -> Option<usize> {
        self.by_id.get(&id).and_then(|(position, _)| *position)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the end is clamped to the message slice and the start comes from its enumeration"
    )]
    pub(crate) fn recent_model_messages(
        &self,
        before_position: usize,
        maximum: usize,
    ) -> impl DoubleEndedIterator<Item = &ConversationMessage> {
        let end = before_position.min(self.messages.len());
        let start = if maximum == 0 {
            end
        } else {
            self.messages[..end]
                .iter()
                .enumerate()
                .rev()
                .filter(|(_, message)| model_visible(&message.kind))
                .nth(maximum - 1)
                .map_or(0, |(position, _)| position)
        };
        self.messages[start..end]
            .iter()
            .filter(|message| model_visible(&message.kind))
    }

    /// Returns one bounded model-visible history delta.
    #[expect(
        clippy::indexing_slicing,
        reason = "partition bounds the message slice"
    )]
    pub(crate) fn model_messages_between(
        &self,
        after: u64,
        through: u64,
        maximum: usize,
    ) -> Result<impl Iterator<Item = &ConversationMessage>> {
        if after < self.resident_after_sequence()
            || after > through
            || through > self.logical_revision()
        {
            return Err(Error::Invalid(
                "model history delta bounds are invalid".into(),
            ));
        }
        let start = self
            .messages
            .partition_point(|message| message.sequence <= after);
        let end = self
            .messages
            .partition_point(|message| message.sequence <= through);
        let messages = self.messages[start..end]
            .iter()
            .filter(|message| model_visible(&message.kind));
        if messages.clone().nth(maximum).is_some() {
            return Err(Error::Invalid(
                "canonical history delta exceeds context limit".into(),
            ));
        }
        Ok(messages)
    }
}
