//! Renderer-independent prompt editor and ratatui input widget.
#[cfg(test)]
acyclic_test_support::counting_allocator!();

#[cfg(not(target_arch = "wasm32"))]
mod completion_popup;
#[cfg(not(target_arch = "wasm32"))]
pub mod config;
#[cfg(not(target_arch = "wasm32"))]
mod diff;
#[cfg(not(target_arch = "wasm32"))]
pub use diff::DiffPage;
#[cfg(not(target_arch = "wasm32"))]
mod feedback;
#[cfg(not(target_arch = "wasm32"))]
pub use feedback::Feedback;
#[cfg(all(not(target_arch = "wasm32"), not(loom)))]
pub mod onboarding;
#[cfg(not(target_arch = "wasm32"))]
mod editor_args;
#[cfg(not(target_arch = "wasm32"))]
mod editor_io;
#[cfg(not(target_arch = "wasm32"))]
mod input_events;
#[cfg(not(target_arch = "wasm32"))]
pub use input_events::WHEEL_ROWS;
#[cfg(not(target_arch = "wasm32"))]
mod markdown;
#[cfg(not(target_arch = "wasm32"))]
pub use markdown::CodeRow;
#[cfg(not(target_arch = "wasm32"))]
mod prompt_box;
#[cfg(not(target_arch = "wasm32"))]
mod prompt_input;
#[cfg(not(target_arch = "wasm32"))]
mod query_controller;
#[cfg(not(target_arch = "wasm32"))]
mod query_page;
#[cfg(not(target_arch = "wasm32"))]
pub use query_controller::QueryController;
#[cfg(not(target_arch = "wasm32"))]
pub use query_page::QueryPage;
#[cfg(not(target_arch = "wasm32"))]
mod hyperlink;
#[cfg(not(target_arch = "wasm32"))]
mod screen;
#[cfg(not(target_arch = "wasm32"))]
mod scrollbar;
#[cfg(not(target_arch = "wasm32"))]
mod styled_cells;
#[cfg(not(target_arch = "wasm32"))]
pub use hyperlink::{Hyperlinks, RowLink};
#[cfg(not(target_arch = "wasm32"))]
mod thinking;
#[cfg(not(target_arch = "wasm32"))]
mod transcript;
#[cfg(not(target_arch = "wasm32"))]
pub use thinking::Thinking;
#[cfg(not(target_arch = "wasm32"))]
pub use transcript::{BlockKind, TranscriptRows};
#[cfg(not(target_arch = "wasm32"))]
mod widget;

#[cfg(not(target_arch = "wasm32"))]
pub use completion_popup::CompletionPopup;
#[cfg(not(target_arch = "wasm32"))]
pub use crossterm;
#[cfg(not(target_arch = "wasm32"))]
pub use editor_args::{editor_command, unix_editor_command, windows_editor_command};
#[cfg(not(target_arch = "wasm32"))]
pub use editor_io::{ClipboardContent, edit_external, prepare_image, prepare_image_with_settings};
#[cfg(not(target_arch = "wasm32"))]
pub use prompt_box::PromptBox;
#[cfg(not(target_arch = "wasm32"))]
pub use prompt_input::{InputEffect, PromptInput};
#[cfg(not(target_arch = "wasm32"))]
pub use ratatui;
#[cfg(not(target_arch = "wasm32"))]
pub use widget::{PromptWidget, Viewport};

#[cfg(not(target_arch = "wasm32"))]
pub use screen::{Screen, ScreenAreas};
#[cfg(not(target_arch = "wasm32"))]
pub use scrollbar::TranscriptScrollbar;
#[cfg(not(target_arch = "wasm32"))]
pub mod term;
