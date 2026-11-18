//! Shared terminal widgets for native clients.
pub use acyclic_harness_tui::*;
pub use acyclic_harness_tui_core::{
    ACTIONS, Action, AppAction, Binding, Cell, Completion, CompletionKind, CompletionMenu,
    CompletionRequest, CompletionWork, DEFAULT_SETTINGS, DecodedKey, Editor, InputKeymap, KEYMAP,
    Layout, Motion, NotificationPolicy, Row, RowPosition, TranscriptAnchor, TranscriptOrigin,
    TranscriptPart, UiSettings, completion_request, decode_key, fuzzy_score, key_chord,
    keymap_help, named_action,
};
pub use acyclic_harness_tui_core::display_text::{display_text, display_text_prefix};
pub use acyclic_harness_tui_core::key_event;

/// Configuration types from the pure model and the renderer.
pub mod config {
    pub use acyclic_harness_tui::config::*;
    pub use acyclic_harness_tui_core::config::*;
}

mod usage;
pub use usage::{context_summary, feedback_summary, usage_summary};
