//! Grapheme-safe prompt editing without a terminal or application dependency.

mod buffer;
#[cfg(feature = "tui")]
mod completion;
#[cfg(feature = "tui")]
mod completion_popup;
mod editor;
#[cfg(all(feature = "tui", not(target_arch = "wasm32")))]
mod editor_args;
#[cfg(all(feature = "tui", not(target_arch = "wasm32")))]
mod editor_io;
mod history;
#[cfg(feature = "tui")]
mod input_events;
#[cfg(feature = "tui")]
mod input_keymap;
#[cfg(feature = "tui")]
mod key_decode;
#[cfg(feature = "tui")]
pub mod key_event;
mod keymap;
mod layout;
#[cfg(feature = "tui")]
mod prompt_box;
#[cfg(feature = "tui")]
mod prompt_files;
#[cfg(feature = "tui")]
mod prompt_input;
#[cfg(feature = "tui")]
mod widget;

#[cfg(feature = "tui")]
pub use completion::{
    Completion, CompletionKind, CompletionMenu, CompletionRequest, CompletionWork,
    completion_request, fuzzy_score,
};
#[cfg(feature = "tui")]
pub use completion_popup::CompletionPopup;
#[cfg(feature = "tui")]
pub use crossterm;
pub use editor::{Editor, Motion};
#[cfg(all(feature = "tui", not(target_arch = "wasm32")))]
pub use editor_args::{editor_command, unix_editor_command, windows_editor_command};
#[cfg(all(feature = "tui", not(target_arch = "wasm32")))]
pub use editor_io::{ClipboardContent, edit_external, prepare_image};
pub use history::PromptHistory;
#[cfg(feature = "tui")]
pub use input_keymap::InputKeymap;
#[cfg(feature = "tui")]
pub use key_decode::{DecodedKey, decode_key, key_chord};
pub use keymap::{Action, Binding, KEYMAP, keymap_help};
pub use layout::{Cell, Layout, Row};
#[cfg(feature = "tui")]
pub use prompt_box::PromptBox;
#[cfg(feature = "tui")]
pub use prompt_files::{
    format_history, load_draft, load_history, parse_history, save_draft, save_history,
};
#[cfg(feature = "tui")]
pub use prompt_input::{InputEffect, PromptInput};
#[cfg(feature = "tui")]
pub use ratatui;
#[cfg(feature = "tui")]
pub use widget::{PromptWidget, Viewport};
