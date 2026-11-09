//! Name ownership is an observation of committed state, not a control transition.

/// A terminal failure with no committed own conversation releases its generation.
#[must_use]
pub const fn released(failed: bool, conversation_tail: u64, conversation_base: u64) -> bool {
    failed && conversation_tail == conversation_base
}
