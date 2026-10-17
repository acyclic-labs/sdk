use acyclic_harness::model::{ReasoningBlock, ReasoningOrigin, ReasoningOutcome};

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
    /// Foreign reasoning becomes explicitly labelled plain context.
    Readable {
        /// Original readable text, without inventing a native block.
        text: &'a str,
        /// Attribution retained for the plain-context delimiter.
        origin: &'a ReasoningOrigin,
        /// Preservation outcome for this request.
        outcome: ReasoningOutcome,
    },
    /// Nothing is sent; retain the decision in this request's projection metadata.
    Dropped {
        /// Why this target received no content.
        outcome: ReasoningOutcome,
    },
}

/// Selects native continuity only within the same known credential scope and family.
/// Native payload shape validation belongs to the target's typed encoder.
pub fn project_reasoning<'a>(
    block: &'a ReasoningBlock,
    target: &ReasoningOrigin,
) -> ProjectedReasoning<'a> {
    let readable = block.text.as_deref().filter(|text| !text.trim().is_empty());
    let compatible = !block.origin.provider.trim().is_empty()
        && block.origin.provider == target.provider
        && block.origin.credential_scope.is_some()
        && block.origin.credential_scope == target.credential_scope;
    if compatible && !block.continuity.is_empty() {
        return ProjectedReasoning::Native {
            continuity: &block.continuity,
            outcome: if block.outcome == ReasoningOutcome::SuppressedDisplay {
                ReasoningOutcome::SuppressedDisplay
            } else if readable.is_some() {
                ReasoningOutcome::PreservedReadable
            } else {
                ReasoningOutcome::PreservedOpaque
            },
        };
    }
    if let Some(text) = readable {
        return ProjectedReasoning::Readable {
            text,
            origin: &block.origin,
            outcome: ReasoningOutcome::PreservedReadable,
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

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "reasoning_tests.rs"]
mod tests;
