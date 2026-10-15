//! Grapheme-safe prompt editing without a terminal or application dependency.

mod buffer;
mod editor;
mod history;
#[cfg(feature = "tui")]
pub mod key_event;
mod keymap;
mod layout;
#[cfg(feature = "tui")]
mod widget;

pub use editor::{Editor, Motion};
pub use history::PromptHistory;
pub use keymap::{Action, Binding, KEYMAP, keymap_help};
pub use layout::{Cell, Layout, Row};
/// Shared renderer types for the host's file-backed panels.
#[cfg(feature = "tui")]
pub use ratatui;
#[cfg(feature = "tui")]
pub use widget::{PromptWidget, Viewport};
