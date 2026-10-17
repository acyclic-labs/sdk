//! Grapheme-safe prompt editing without a terminal or application dependency.

mod buffer;
#[cfg(feature = "tui")]
mod completion;
#[cfg(feature = "tui")]
mod completion_popup;
#[cfg(feature = "tui")]
pub mod config;
#[cfg(feature = "tui")]
mod display_text;
#[cfg(feature = "tui")]
mod feedback;
#[cfg(feature = "tui")]
pub use feedback::Feedback;
mod editor;
#[cfg(feature = "tui")]
pub use display_text::display_text;
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
mod settings;
pub use settings::{DEFAULT_SETTINGS, NotificationPolicy, UiSettings};
#[cfg(feature = "tui")]
mod prompt_box;
#[cfg(feature = "tui")]
mod prompt_files;
#[cfg(feature = "tui")]
mod prompt_input;
#[cfg(feature = "tui")]
mod screen;
#[cfg(feature = "tui")]
mod widget;
#[cfg(feature = "tui")]
mod wrapped_lines;

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
pub use editor_io::{ClipboardContent, edit_external, prepare_image, prepare_image_with_settings};
pub use history::PromptHistory;
#[cfg(feature = "tui")]
pub use input_keymap::InputKeymap;
#[cfg(feature = "tui")]
pub use key_decode::{DecodedKey, decode_key, key_chord};
pub use keymap::{ACTIONS, Action, AppAction, Binding, KEYMAP, keymap_help, named_action};
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
#[cfg(feature = "tui")]
pub use wrapped_lines::WrappedLines;

#[cfg(feature = "tui")]
pub use screen::{Screen, ScreenAreas};
#[cfg(feature = "tui")]
pub mod term;
