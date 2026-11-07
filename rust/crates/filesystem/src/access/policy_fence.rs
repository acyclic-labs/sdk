/// Persisted policy identity and expiry captured by an authorized view.
/// A transaction observes this identity before staging, and checks its expiry
/// again at publication. Unrelated data changes can then rebase normally.
#[derive(Clone, Copy)]
pub struct PolicyFence {
    pub(super) generation: crate::GenerationId,
    pub(super) revision: crate::kernel::MetadataField<crate::ObjectId>,
    pub(super) generation_sensitive: bool,
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

    /// The captured decision's exclusive expiry in Unix seconds, when bounded.
    #[must_use]
    pub const fn valid_until(self) -> Option<u64> {
        self.valid_until
    }

    /// Whether the captured decision has not expired. Every time condition is
    /// an expiry, so a decision valid when made stays valid at any earlier
    /// reading: a wall clock stepped back never revokes live authority.
    #[must_use]
    pub fn valid_at(self, now: u64) -> bool {
        self.valid_until.is_none_or(|expiry| now < expiry)
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
