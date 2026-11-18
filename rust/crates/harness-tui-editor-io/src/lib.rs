//! External editor handoff and clipboard attachment preparation.
#![cfg(not(target_arch = "wasm32"))]

mod editor_args;
mod editor_io;

pub use editor_args::{editor_command, unix_editor_command, windows_editor_command};
pub use editor_io::{ClipboardContent, edit_external, prepare_image, prepare_image_with_settings};
