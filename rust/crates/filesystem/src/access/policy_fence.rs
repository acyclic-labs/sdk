/// Persisted policy identity and time interval captured by an authorized view.
/// A transaction observes this identity before staging, and checks its interval
/// again at publication. Unrelated data changes can then rebase normally.
#[derive(Clone, Copy)]
pub struct PolicyFence {
    pub(super) generation: crate::GenerationId,
    pub(super) revision: crate::kernel::MetadataField<crate::ObjectId>,
    pub(super) generation_sensitive: bool,
    pub(super) valid_from: u64,
    pub(super) valid_until: Option<u64>,
}

impl PolicyFence {
    /// Keeps an explicit expected-generation precondition at publication.
    #[must_use]
    pub const fn pin_generation(mut self) -> Self {
        self.generation_sensitive = true;
        self
    }

    /// Whether an unrelated generation change requires a fresh authorization.
    #[must_use]
    pub const fn generation_sensitive(self) -> bool {
        self.generation_sensitive
    }

    /// Whether the captured decision is still within its original time interval.
    #[must_use]
    pub fn valid_at(self, now: u64) -> bool {
        now >= self.valid_from && self.valid_until.is_none_or(|expiry| now < expiry)
    }

    pub(crate) fn matches(
        self,
        generation: crate::GenerationId,
        revision: crate::kernel::MetadataField<crate::ObjectId>,
        now: u64,
    ) -> bool {
        self.revision == revision
            && (!self.generation_sensitive || self.generation == generation)
            && self.valid_at(now)
    }
}
