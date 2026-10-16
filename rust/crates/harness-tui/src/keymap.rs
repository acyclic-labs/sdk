use crate::Motion;

/// Declarative action; application actions are returned to the host router.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Move the cursor; Shift extends selection.
    Move(Motion),
    /// Delete a grapheme (true means backwards).
    Delete(bool),
    /// Delete a word (true means backwards).
    DeleteWord(bool),
    /// Kill into the ring.
    Kill(Motion),
    /// Select all text.
    SelectAll,
    /// Undo an edit group.
    Undo,
    /// Redo an edit group.
    Redo,
    /// Insert the newest kill.
    Yank,
    /// Rotate the preceding yank.
    RotateYank,
    /// Submit, or continue after a backslash.
    Submit,
    /// Insert a newline.
    Newline,
    /// Complete or indent.
    Tab,
    /// Insert a literal tab.
    LiteralTab,
    /// Cycle the dock backwards.
    PreviousDock,
    /// Copy selection, otherwise clear/interrupt; twice quits.
    CopyOrInterrupt,
    /// Cut selection; begin the external-editor chord.
    CutPrefix,
    /// Read clipboard contents.
    Paste,
    /// Search prompt history backwards.
    ReverseSearch,
    /// Run the external editor.
    ExternalEditor,
    /// Delete forwards, or quit when empty.
    DeleteOrQuit,
    /// Delegate the escape ladder to the host.
    Escape,
    /// Scroll transcript upwards.
    TranscriptUp,
    /// Scroll transcript downwards.
    TranscriptDown,
    /// Toggle host voice dictation.
    Voice,
}

/// Binding data shared by dispatch, overrides and generated help.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Binding {
    /// Canonical modifier order: Ctrl, Alt, Shift, Super.
    pub chord: &'static str,
    /// Dispatched action.
    pub action: Action,
    /// Help text.
    pub help: &'static str,
    /// Whether a terminal repeat event may invoke this action.
    pub repeat: bool,
}

const fn bind(chord: &'static str, action: Action, help: &'static str, repeat: bool) -> Binding {
    Binding {
        chord,
        action,
        help,
        repeat,
    }
}

/// Default keymap. Shift extends any motion. The application must reserve these
/// while input is focused; Ctrl+Y and Ctrl+K are exclusively editor actions.
pub const KEYMAP: &[Binding] = &[
    bind("Ctrl+;", Action::Voice, "Toggle voice dictation", false),
    bind(
        "Left",
        Action::Move(Motion::Left),
        "Previous grapheme",
        true,
    ),
    bind("Right", Action::Move(Motion::Right), "Next grapheme", true),
    bind(
        "Up",
        Action::Move(Motion::Up),
        "Previous row; history at buffer start",
        true,
    ),
    bind(
        "Down",
        Action::Move(Motion::Down),
        "Next row; history at buffer end",
        true,
    ),
    bind("Home", Action::Move(Motion::LineStart), "Line start", true),
    bind("End", Action::Move(Motion::LineEnd), "Line end", true),
    bind(
        "Ctrl+Home",
        Action::Move(Motion::Start),
        "Buffer start",
        true,
    ),
    bind("Ctrl+End", Action::Move(Motion::End), "Buffer end", true),
    bind(
        "Ctrl+Left",
        Action::Move(Motion::WordLeft),
        "Previous word",
        true,
    ),
    bind(
        "Ctrl+Right",
        Action::Move(Motion::WordRight),
        "Next word",
        true,
    ),
    bind(
        "Alt+Left",
        Action::Move(Motion::WordLeft),
        "Previous word",
        true,
    ),
    bind(
        "Alt+Right",
        Action::Move(Motion::WordRight),
        "Next word",
        true,
    ),
    bind(
        "Super+Left",
        Action::Move(Motion::LineStart),
        "Line start",
        true,
    ),
    bind(
        "Super+Right",
        Action::Move(Motion::LineEnd),
        "Line end",
        true,
    ),
    bind(
        "Super+Up",
        Action::Move(Motion::Start),
        "Buffer start",
        true,
    ),
    bind("Super+Down", Action::Move(Motion::End), "Buffer end", true),
    bind(
        "Alt+b",
        Action::Move(Motion::WordLeft),
        "Previous word",
        true,
    ),
    bind("Alt+f", Action::Move(Motion::WordRight), "Next word", true),
    bind(
        "Backspace",
        Action::Delete(true),
        "Delete previous grapheme",
        true,
    ),
    bind(
        "Delete",
        Action::Delete(false),
        "Delete next grapheme",
        true,
    ),
    bind(
        "Ctrl+Backspace",
        Action::DeleteWord(true),
        "Delete previous word",
        true,
    ),
    bind(
        "Alt+Backspace",
        Action::DeleteWord(true),
        "Delete previous word",
        true,
    ),
    bind(
        "Ctrl+Delete",
        Action::DeleteWord(false),
        "Delete next word",
        true,
    ),
    bind("Alt+d", Action::DeleteWord(false), "Delete next word", true),
    bind(
        "Super+Backspace",
        Action::Kill(Motion::LineStart),
        "Kill to line start",
        true,
    ),
    bind("Ctrl+a", Action::SelectAll, "Select all", false),
    bind("Ctrl+z", Action::Undo, "Undo", false),
    bind("Ctrl+Shift+z", Action::Redo, "Redo", false),
    bind(
        "Ctrl+k",
        Action::Kill(Motion::LineEnd),
        "Kill to line end",
        true,
    ),
    bind(
        "Ctrl+u",
        Action::Kill(Motion::LineStart),
        "Kill to line start",
        true,
    ),
    bind(
        "Ctrl+w",
        Action::Kill(Motion::WordLeft),
        "Kill previous word",
        true,
    ),
    bind("Ctrl+y", Action::Yank, "Yank", false),
    bind("Alt+y", Action::RotateYank, "Rotate yank", false),
    bind(
        "Ctrl+r",
        Action::ReverseSearch,
        "Reverse prompt search",
        false,
    ),
    bind(
        "Ctrl+c",
        Action::CopyOrInterrupt,
        "Copy selection; otherwise clear/interrupt; twice quits",
        false,
    ),
    bind(
        "Ctrl+d",
        Action::DeleteOrQuit,
        "Delete forwards; quit when empty",
        false,
    ),
    bind(
        "Ctrl+x",
        Action::CutPrefix,
        "Cut; Ctrl+E next opens $EDITOR",
        false,
    ),
    bind(
        "Ctrl+x Ctrl+e",
        Action::ExternalEditor,
        "External editor",
        false,
    ),
    bind(
        "Ctrl+v",
        Action::Paste,
        "Paste text or clipboard image",
        false,
    ),
    bind(
        "Enter",
        Action::Submit,
        "Submit; trailing backslash continues",
        false,
    ),
    bind("Shift+Enter", Action::Newline, "Insert newline", false),
    bind("Alt+Enter", Action::Newline, "Insert newline", false),
    bind("Ctrl+Enter", Action::Newline, "Insert newline", false),
    bind(
        "Tab",
        Action::Tab,
        "Accept completion or indent; host may route dock",
        false,
    ),
    bind("Alt+Tab", Action::LiteralTab, "Insert tab", false),
    bind(
        "Shift+Tab",
        Action::PreviousDock,
        "Previous dock item",
        false,
    ),
    bind(
        "Esc",
        Action::Escape,
        "Cancel input mode or host escape ladder",
        false,
    ),
    bind("PageUp", Action::TranscriptUp, "Scroll transcript up", true),
    bind(
        "PageDown",
        Action::TranscriptDown,
        "Scroll transcript down",
        true,
    ),
];

/// Generate help directly from dispatch data.
pub fn keymap_help(bindings: &[Binding]) -> String {
    bindings
        .iter()
        .map(|b| format!("{}\t{}\n", b.chord, b.help))
        .collect()
}
