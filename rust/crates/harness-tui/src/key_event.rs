//! Typed events shared by terminal decoders and the prompt editor. This module
//! has no escape-sequence parsing or terminal setup responsibilities.

pub use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};

/// Input already decoded by the terminal layer. IME commits stay distinct from
/// paste so the editor never applies paste heuristics or folding to them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputEvent {
    /// A decoded key press, repeat or release with its complete modifiers.
    Key(KeyEvent),
    /// A complete, explicitly identified paste.
    Paste(String),
    /// Text committed by an IME or another text input service.
    CommittedText(String),
    /// A decoded mouse event in terminal cell coordinates.
    Mouse(MouseEvent),
    /// Whether the terminal gained focus.
    Focus(bool),
    /// New terminal width and height in cells.
    Resize(u16, u16),
}
