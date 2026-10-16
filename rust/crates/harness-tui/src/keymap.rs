use crate::Motion;

/// Declarative action; application actions are returned to the host router.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Move the cursor; Shift extends selection.
    Move(Motion),
    /// Extend the selection, independently of the physical Shift modifier.
    Select(Motion),
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
    /// Accept completion, submit, or continue after a backslash.
    Submit,
    /// Insert a newline.
    Newline,
    /// Complete or indent.
    Tab,
    /// Insert a literal tab.
    LiteralTab,
    /// Cycle completion or dock items backwards.
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
    /// Host-owned application action.
    App(AppAction),
}

/// Binding data shared by dispatch, overrides and generated help.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Binding<'a> {
    /// Canonical modifier order: Ctrl, Alt, Shift, Super.
    pub chord: &'a str,
    /// Dispatched action.
    pub action: Action,
    /// Help text.
    pub help: &'static str,
    /// Whether a terminal repeat event may invoke this action.
    pub repeat: bool,
}

include!(concat!(env!("OUT_DIR"), "/keymap.rs"));

/// A host action selected by its stable public configuration name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppAction {
    /// Quit application action.
    Quit,
    /// StopAll application action.
    StopAll,
    /// ResumeAll application action.
    ResumeAll,
    /// NewAgent application action.
    NewAgent,
    /// ClosePanel application action.
    ClosePanel,
    /// NextMessage application action.
    NextMessage,
    /// PreviousMessage application action.
    PreviousMessage,
    /// OpenChanges application action.
    OpenChanges,
    /// ToggleFollowTail application action.
    ToggleFollowTail,
    /// Picker application action.
    Picker,
    /// NewSession application action.
    NewSession,
    /// DeleteSession application action.
    DeleteSession,
    /// Reasoning application action.
    Reasoning,
    /// Details application action.
    Details,
    /// Diff application action.
    Diff,
    /// Tree application action.
    Tree,
    /// Terminal application action.
    Terminal,
    /// Find application action.
    Find,
    /// Pause application action.
    Pause,
    /// Stop application action.
    Stop,
    /// Resume application action.
    Resume,
    /// Mode application action.
    Mode,
    /// ClearAll application action.
    ClearAll,
    /// AttentionNext application action.
    AttentionNext,
    /// AttentionPrevious application action.
    AttentionPrevious,
    /// Theme application action.
    Theme,
    /// Export application action.
    Export,
    /// Feedback application action.
    Feedback,
    /// Open application action.
    Open,
    /// Shell application action.
    Shell,
    /// Delegate application action.
    Delegate,
    /// Search application action.
    Search,
    /// Fetch application action.
    Fetch,
    /// Compact application action.
    Compact,
    /// Model application action.
    Model,
    /// Plan application action.
    Plan,
    /// Goal application action.
    Goal,
    /// Mcp application action.
    Mcp,
    /// Sessions application action.
    Sessions,
    /// Parent application action.
    Parent,
    /// Child application action.
    Child,
    /// SiblingNext application action.
    SiblingNext,
    /// SiblingPrevious application action.
    SiblingPrevious,
    /// NavigationBack application action.
    NavigationBack,
    /// NavigationForward application action.
    NavigationForward,
    /// Help application action.
    Help,
    /// FocusMessages application action.
    FocusMessages,
    /// FocusInput application action.
    FocusInput,
    /// ToggleWrap application action.
    ToggleWrap,
    /// ToggleNotifications application action.
    ToggleNotifications,
}

/// Look up one action by its declarative name, independently of its key bindings.
pub fn named_action(name: &str) -> Option<Binding<'static>> {
    ACTIONS
        .iter()
        .find(|binding| binding.chord == name)
        .copied()
}

impl Action {
    /// Stable name for configuration and host dispatch.
    pub fn name(self) -> &'static str {
        ACTIONS
            .iter()
            .find(|binding| binding.action == self)
            .map_or("unsupported", |binding| binding.chord)
    }
}

/// Generate help directly from dispatch data.
pub fn keymap_help(bindings: &[Binding<'_>]) -> String {
    bindings
        .iter()
        .map(|b| format!("{}\t{}\n", b.chord, b.help))
        .collect()
}
