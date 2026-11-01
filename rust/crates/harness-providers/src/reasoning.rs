use acyclic_harness_core::model::{NativePart, ReasoningBlock, ReasoningOrigin, ReasoningOutcome};

/// Request-local decision; the canonical block and its bytes remain unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectedReasoning<'a> {
    /// The adapter must validate and replay this native payload in its original position.
    Native {
        /// Exact borrowed continuity, with no decoding or re-encoding here.
        continuity: &'a [u8],
        /// Preservation outcome for this request.
        outcome: ReasoningOutcome,
    },
    /// Nothing is sent; retain the decision in this request's projection metadata.
    Dropped {
        /// Why this target received no content.
        outcome: ReasoningOutcome,
    },
}

/// Whether native bytes from `origin` replay to `target`: only within the same
/// known credential scope and family.
fn compatible(origin: &ReasoningOrigin, target: &ReasoningOrigin) -> bool {
    !origin.provider.trim().is_empty()
        && origin.provider == target.provider
        && origin.credential_scope.is_some()
        && origin.credential_scope == target.credential_scope
}

/// The exact native bytes of any assistant part that `target` may replay;
/// none means the target receives the canonical part instead.
pub fn project_native<'a>(
    native: Option<&'a NativePart>,
    target: &ReasoningOrigin,
) -> Option<&'a [u8]> {
    native
        .filter(|native| compatible(&native.origin, target) && !native.continuity.is_empty())
        .map(|native| native.continuity.as_slice())
}

/// Selects native continuity only within the same known credential scope and family.
/// Native payload shape validation belongs to the target's typed encoder.
pub fn project_reasoning<'a>(
    block: &'a ReasoningBlock,
    target: &ReasoningOrigin,
) -> ProjectedReasoning<'a> {
    let readable = block.text.as_deref().filter(|text| !text.trim().is_empty());
    if compatible(&block.origin, target) && !block.continuity.is_empty() {
        return ProjectedReasoning::Native {
            continuity: &block.continuity,
            outcome: if readable.is_some() {
                ReasoningOutcome::PreservedReadable
            } else {
                ReasoningOutcome::PreservedOpaque
            },
        };
    }
    ProjectedReasoning::Dropped {
        outcome: if block.continuity.is_empty() {
            ReasoningOutcome::MissingContent
        } else {
            ReasoningOutcome::UnsupportedShape
        },
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "reasoning_tests.rs"]
mod tests;
