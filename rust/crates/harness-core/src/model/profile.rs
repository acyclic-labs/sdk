//! Sealed projection of an admitted profile, without caller option maps.
use acyclic_model_render::{Cache, Effort, Reasoning};

/// Immutable provider-facing values obtained only from an admitted agent profile.
/// The original epoch owner persists the identities; this value creates no epoch,
/// authority, request, credential source or model call.
#[derive(Clone, Copy, Debug)]
pub struct ModelProfileBinding {
    pub(crate) digest: [u8; 32],
    pub(crate) prefix: [u8; 32],
    pub(crate) reasoning: Reasoning,
    pub(crate) cache: Cache,
    pub(crate) max_output_tokens: Option<u32>,
    pub(crate) capacity: crate::context::ModelContextCapacity,
    pub(crate) overlay_effort: Option<Effort>,
}

impl ModelProfileBinding {
    /// Full profile identity for the existing checkpoint publication.
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    /// Prefix identity, independent of role directives and availability.
    pub const fn prefix_digest(&self) -> [u8; 32] {
        self.prefix
    }
    /// Frozen reasoning baseline; a role override is an appended item.
    pub const fn reasoning(&self) -> Reasoning {
        self.reasoning
    }
    /// Admitted cache semantics, retained independently of a transient request.
    pub const fn cache(&self) -> Cache {
        self.cache
    }
    /// Role reservation; absent means the selected model's verified capacity.
    pub const fn max_output_tokens(&self) -> Option<u32> {
        self.max_output_tokens
    }
    /// Intersection of the user's context reservation and provider hard limits.
    pub const fn capacity(&self) -> crate::context::ModelContextCapacity {
        self.capacity
    }
    /// Effort changes are appended items, never replacements of the baseline.
    pub const fn overlay_effort(&self) -> Option<Effort> {
        self.overlay_effort
    }
}
