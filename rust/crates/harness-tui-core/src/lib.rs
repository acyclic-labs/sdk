//! Prompt editing, completion, and configuration state shared by the harness TUI.

#[cfg(test)]
acyclic_test_support::counting_allocator!();

mod buffer;
mod editor;
mod history;
mod keymap;
mod layout;
mod settings;

pub use editor::{Editor, Motion};
pub use keymap::{ACTIONS, Action, AppAction, Binding, KEYMAP, keymap_help, named_action};
pub use layout::{Cell, Layout, Row, Viewport};
pub use settings::{DEFAULT_SETTINGS, NotificationPolicy, UiSettings};

#[cfg(not(target_arch = "wasm32"))]
mod completion;
#[cfg(not(target_arch = "wasm32"))]
pub mod config;
#[cfg(not(target_arch = "wasm32"))]
mod display_text;
#[cfg(not(target_arch = "wasm32"))]
mod input_keymap;
#[cfg(not(target_arch = "wasm32"))]
mod key_decode;
#[cfg(not(target_arch = "wasm32"))]
pub mod key_event;
#[cfg(not(target_arch = "wasm32"))]
mod transcript_anchor;

#[cfg(not(target_arch = "wasm32"))]
pub use completion::{
    Completion, CompletionKind, CompletionMenu, CompletionRequest, CompletionWork,
    completion_request, fuzzy_score,
};
#[cfg(not(target_arch = "wasm32"))]
pub use display_text::{display_text, display_text_prefix};
#[cfg(not(target_arch = "wasm32"))]
pub use input_keymap::InputKeymap;
#[cfg(not(target_arch = "wasm32"))]
pub use key_decode::{ChordContinuation, DecodedKey, PendingChord, decode_key, key_chord};
#[cfg(not(target_arch = "wasm32"))]
pub use transcript_anchor::{RowPosition, TranscriptAnchor, TranscriptOrigin, TranscriptPart};
