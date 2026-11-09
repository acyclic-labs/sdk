//! Name ownership is an observation of committed state, not a control transition.

/// A terminal failure before any own reply or agent action releases its generation.
#[must_use]
pub const fn released(
    failed: bool,
    conversation_tail: u64,
    conversation_base: u64,
    committed_actions: bool,
) -> bool {
    failed && conversation_tail == conversation_base && !committed_actions
}
