//! Pure prompt editing, keymaps, capabilities and shipped defaults.
#[cfg(test)]
acyclic_test_support::counting_allocator!();

/// Grapheme-safe prompt text and offset helpers.
pub mod buffer;
#[cfg(not(target_arch = "wasm32"))]
mod completion;
/// Declarative configuration files, schema checks and precedence.
#[cfg(not(target_arch = "wasm32"))]
pub mod config;
/// Printable text derived from untrusted terminal bytes.
pub mod display_text;
mod editor;
mod history;
/// Canonical keymap overrides and their resolution.
#[cfg(not(target_arch = "wasm32"))]
pub mod input_keymap;
/// Decoding terminal keys through the declarative keymap.
#[cfg(not(target_arch = "wasm32"))]
pub mod key_decode;
/// Typed input events shared by decoders and the editor.
#[cfg(not(target_arch = "wasm32"))]
pub mod key_event;
/// Declarative actions and their default bindings.
pub mod keymap;
/// Grapheme-safe display layout for the prompt.
pub mod layout;
/// Pure view and editor preferences.
pub mod settings;
/// Persisted transcript anchors and their shared wire oracle.
pub mod transcript_anchor;
mod theme;

#[cfg(not(target_arch = "wasm32"))]
pub use completion::{
    Completion, CompletionKind, CompletionMenu, CompletionRequest, CompletionWork,
    completion_request, fuzzy_score,
};
pub use editor::{Editor, Motion};
#[cfg(not(target_arch = "wasm32"))]
pub use input_keymap::InputKeymap;
#[cfg(not(target_arch = "wasm32"))]
pub use key_decode::{DecodedKey, decode_key, key_chord};
pub use keymap::{ACTIONS, Action, AppAction, Binding, KEYMAP, keymap_help, named_action};
pub use layout::{Cell, Layout, Row};
pub use settings::{DEFAULT_SETTINGS, NotificationPolicy, UiSettings};
pub use theme::Token;
pub use transcript_anchor::{RowPosition, TranscriptAnchor, TranscriptOrigin, TranscriptPart};
