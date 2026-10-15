//! Grapheme-safe prompt editing without a terminal or application dependency.

mod buffer;
mod editor;
mod history;
mod keymap;
mod layout;
#[cfg(feature = "tui")]
pub mod key_event;
#[cfg(feature = "tui")]
mod widget;

pub use editor::{Editor, Motion};
pub use history::PromptHistory;
pub use keymap::{Action, Binding, KEYMAP, keymap_help};
pub use layout::{Cell, Layout, Row};
#[cfg(feature = "tui")]
pub use widget::{PromptWidget, Viewport};
