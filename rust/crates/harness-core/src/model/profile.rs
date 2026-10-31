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

    /// A fixed role override, distinct from the frozen model policy. Adaptive
    /// roles retain that policy; they never send an unsupported `adaptive`
    /// literal to a protocol's fixed-effort field.
    pub fn fixed_effort(&self) -> Option<Effort> {
        self.overlay_effort.filter(|effort| {
            *effort != Effort::Adaptive
                && match self.reasoning {
                    Reasoning::Effort(baseline)
                    | Reasoning::Adaptive(baseline)
                    | Reasoning::Level(baseline) => *effort != baseline,
                    Reasoning::Disabled => true,
                }
        })
    }

    /// The original conversation owner appends this item once at the role
    /// transition and retains it on replay. It is never prepended by a provider
    /// on each request, and does not replace the epoch's reasoning baseline.
    pub fn configuration_update(&self, previous: Option<Effort>) -> Option<super::ModelMessage> {
        let baseline = self.baseline_effort()?;
        let effort = self.fixed_effort().unwrap_or(baseline);
        if previous.unwrap_or(baseline) == effort {
            return None;
        }
        Some(super::ModelMessage {
            role: super::ModelRole::User,
            content: super::ModelContent::Part(super::ModelContentPart::Configuration {
                prefix: self.prefix,
                effort,
            }),
        })
    }

    /// Concrete baseline retained in the epoch header.
    pub const fn baseline_effort(&self) -> Option<Effort> {
        match self.reasoning {
            Reasoning::Effort(effort) | Reasoning::Adaptive(effort) | Reasoning::Level(effort) => {
                Some(effort)
            }
            Reasoning::Disabled => None,
        }
    }
}

impl crate::heap_layout::HeapLayout for Effort {
    const LAYOUT: &'static crate::heap_layout::JsonLayout = &crate::heap_layout::JsonLayout::Scalar;
}
