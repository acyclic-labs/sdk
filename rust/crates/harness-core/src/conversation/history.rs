use super::*;

impl ConversationState {}

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

    pub(crate) fn checkpoint_covers_archived_prefix(
        &self,
        reference: &FileRef,
        through: u64,
    ) -> bool {
        self.archived_prefix.as_ref().is_some_and(|prefix| {
            prefix.checkpoint == *reference && prefix.through_sequence == through
        })
    }
}
