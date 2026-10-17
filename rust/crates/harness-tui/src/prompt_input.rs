use crate::key_decode::{DecodedKey, decode_key, event_chord};
use crate::key_event::{
    KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crate::{
    Action, CompletionMenu, Editor, InputKeymap, Motion, PromptHistory, Viewport,
    history::Navigation,
};
use ratatui::layout::Rect;
use unicode_segmentation::UnicodeSegmentation;

/// Effects requiring application context or operating-system access.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputEffect {
    /// A complete submitted prompt, with pastes expanded.
    Submit {
        /// Expanded prompt text.
        text: String,
        /// Whether the host should show its queued-message badge.
        queued: bool,
        /// Existing user-message identifier when replacing a message.
        edit: Option<String>,
    },
    /// Copy text through the host clipboard adapter.
    Copy(String),
    /// Read text or image data from the clipboard.
    Paste,
    /// Suspend terminal presentation and edit the current text externally.
    ExternalEditor,
    /// Move through transcript matches; the host publishes the selection file.
    FindStep {
        /// Move to the preceding match instead of the following one.
        previous: bool,
    },
    /// Interrupt the active operation.
    Interrupt,
    /// Exit requested by double interrupt or Ctrl+D on an empty prompt.
    Quit,
    /// Host-owned dock, transcript, and escape actions.
    Host(Action),
}

#[derive(Clone, Debug)]
struct Search {
    original: Editor,
    navigation: Navigation,
    query: String,
    matched: bool,
}

#[derive(Clone, Debug)]
struct MessageEdit {
    message: String,
    navigation: Navigation,
    draft: Editor,
}

/// Prompt controller over typed input events. IME commits always insert text,
/// regardless of size; terminal protocol handling belongs to `term`.
#[derive(Clone, Debug, Default)]
pub struct PromptInput {
    /// Editable prompt state.
    pub editor: Editor,
    /// Semantic UI roles resolved against the detected terminal profile.
    pub theme: crate::config::ThemeStyles,
    /// Shared input-history file projection; load/save with the history codecs.
    /// Message edits and search previews never own or replace these entries.
    history: PromptHistory,
    /// Whether a submission is queued behind active host work.
    pub busy: bool,
    /// Whether Tab and empty-input Up/Down navigate approval/question items.
    pub dock_items: bool,
    /// Whether pasting opens an approval/question's comment mode.
    pub question: bool,
    /// Comment mode, retained by the host when switching item drafts.
    pub comment_mode: bool,
    /// Number of clipboard images already attached by the host.
    pub images: usize,
    /// Whether the host voice recorder is active.
    pub recording: bool,
    /// Host transcript selection, already converted to markdown for copying.
    pub transcript_selection: Option<String>,
    /// Followed transcript-search form owns this editor instead of chat.
    pub transcript_search: bool,
    /// User overrides used by both dispatch and help.
    pub keymap: InputKeymap,
    /// Completion results folded from the host's current file-query snapshots.
    pub completion: CompletionMenu,
    pending_chord: Option<(String, u64, Option<DecodedKey>)>,
    last_interrupt: Option<u64>,
    search: Option<Search>,
    editing: Option<MessageEdit>,
    click: Option<(u16, u16, u64, u8)>,
    pub(crate) dragging: bool,
    pub(crate) input_width: usize,
    last_activity: u64,
}

impl PromptInput {
    /// Replace followed bindings. Deferred text remains committed input; actions
    /// from an obsolete map (including a pending cut) are cancelled.
    pub fn set_keymap(&mut self, keymap: InputKeymap) {
        if self.keymap == keymap {
            return;
        }
        let pending = self.pending_chord.take();
        self.keymap = keymap;
        self.last_interrupt = None;
        if let Some((_, at, Some(DecodedKey::Text(text)))) = pending {
            // There is no pending chord left to flush into a host effect.
            let _ = self.committed_text(text.encode_utf8(&mut [0; 4]), at);
        }
    }

    /// Apply preferences to active and suspended drafts so cancelling an edit or
    /// search cannot resurrect preferences from an older file snapshot.
    pub fn set_settings(&mut self, settings: crate::UiSettings) -> bool {
        if !self.editor.set_settings(settings) {
            return false;
        }
        if let Some(search) = self.search.as_mut() {
            search.original.set_settings(settings);
        }
        if let Some(edit) = self.editing.as_mut() {
            edit.draft.set_settings(settings);
        }
        true
    }

    /// Current history-file projection, shared by every editor context.
    pub fn history(&self) -> &PromptHistory {
        &self.history
    }

    /// Fold a loaded history-file snapshot, invalidating older navigation.
    pub fn replace_history(&mut self, snapshot: PromptHistory) {
        self.history.replace(snapshot);
    }

    /// Refresh the popup from the editor delta and current file-query fold.
    pub fn refresh_completions(&mut self) {
        if self.transcript_search {
            self.completion.dismiss(&self.editor);
        } else {
            self.completion.refresh(&self.editor);
        }
    }
    /// Build a controller around a loaded draft or new editor.
    pub fn new(editor: Editor) -> Self {
        Self {
            editor,
            ..Self::default()
        }
    }

    /// Start editing a previous user message, preserving the live draft.
    pub fn edit_message(&mut self, message: String, text: &str) -> Vec<InputEffect> {
        let effects = self.flush_chord();
        self.last_interrupt = None;
        self.finish_search(self.last_activity, false);
        if let Some(editing) = self.editing.take() {
            self.editor = editing.draft;
            self.history.restore_navigation(editing.navigation);
        }
        self.editing = Some(MessageEdit {
            message,
            navigation: self.history.navigation.clone(),
            draft: self.editor.clone(),
        });
        self.history.reset();
        let settings = *self.editor.settings();
        self.editor = Editor::new(text);
        self.editor.set_settings(settings);
        effects
    }

    /// Message identifier for an active edit, for focus and border styling.
    pub fn editing_message(&self) -> Option<&str> {
        self.editing.as_ref().map(|edit| edit.message.as_str())
    }

    /// Apply a successful external edit as one undo step; cancellation is inert.
    pub fn external_result(&mut self, text: Option<&str>, now_ms: u64) -> Vec<InputEffect> {
        let effects = self.flush_chord();
        if let Some(text) = text {
            self.last_interrupt = None;
            self.finish_search(now_ms, true);
            self.editor.set_text(text, now_ms);
            self.last_activity = now_ms;
        }
        effects
    }

    /// Latest input activity, for status-dot and animation hooks.
    pub fn last_activity(&self) -> u64 {
        self.last_activity
    }

    /// Opacity for a bar cursor: input resets it to fully visible, then it fades.
    pub fn cursor_opacity(&self, now_ms: u64, focused: bool) -> u8 {
        if !focused {
            return 0;
        }
        let elapsed = now_ms.saturating_sub(self.last_activity);
        let idle = self.editor.settings().cursor_idle_ms as u64;
        let cycle = self.editor.settings().cursor_cycle_ms as u64;
        let half = cycle / 2;
        if elapsed < idle {
            return 255;
        }
        let phase = (elapsed - idle) % cycle;
        ((if phase < half {
            half - phase
        } else {
            phase - half
        }) * 255
            / half) as u8
    }

    /// Current reverse-search query and whether a history entry matched it.
    pub fn search_status(&self) -> Option<(&str, bool)> {
        self.search
            .as_ref()
            .map(|search| (search.query.as_str(), search.matched))
    }

    /// Accept one whole committed text event. No timing heuristic is applied.
    pub fn committed_text(&mut self, text: &str, now_ms: u64) -> Vec<InputEffect> {
        let effects = self.flush_chord();
        self.last_interrupt = None;
        self.last_activity = now_ms;
        if let Some(search) = self.search.as_mut() {
            search.query.push_str(text);
            self.history.reset();
            self.search_previous();
        } else {
            self.editor.insert(text, now_ms);
        }
        effects
    }

    /// Accept bracketed paste or explicitly identified clipboard text.
    pub fn paste(&mut self, text: &str, now_ms: u64) -> Vec<InputEffect> {
        let effects = self.flush_chord();
        self.last_interrupt = None;
        if self.question {
            self.comment_mode = true;
        }
        self.finish_search(now_ms, true);
        self.editor.paste(text, now_ms);
        self.last_activity = now_ms;
        effects
    }

    /// Flush a timed-out Ctrl+X prefix, preserving ordinary cut semantics.
    pub(crate) fn chord_timeout(&mut self, now_ms: u64) -> Vec<InputEffect> {
        if self.pending_chord.as_ref().is_some_and(|(_, at, _)| {
            now_ms.saturating_sub(*at) >= self.editor.settings().chord_timeout_ms as u64
        }) {
            self.flush_chord()
        } else {
            vec![]
        }
    }

    pub(crate) fn flush_chord(&mut self) -> Vec<InputEffect> {
        self.pending_chord
            .take()
            .map_or_else(Vec::new, |(_, at, decoded)| {
                decoded.map_or_else(Vec::new, |decoded| {
                    self.apply_decoded(decoded, at, self.input_width)
                })
            })
    }

    /// Process a crossterm key. Page keys and application escape routing remain
    /// host effects, so editor shortcuts cannot accidentally scroll the input.
    pub fn key(&mut self, key: KeyEvent, now_ms: u64, width: usize) -> Vec<InputEffect> {
        self.input_width = width;
        if key.kind == KeyEventKind::Release {
            return vec![];
        }
        let mut effects = self.chord_timeout(now_ms);
        if key.kind == KeyEventKind::Repeat
            && self.pending_chord.as_ref().is_some_and(|(prefix, _, _)| {
                event_chord(key).is_some_and(|chord| chord.as_str() == prefix)
            })
        {
            return effects;
        }
        let decoded = decode_key(key, &self.keymap);
        if let Some((prefix, _, _)) = self.pending_chord.as_ref() {
            let chord = event_chord(key).map(|chord| format!("{prefix} {chord}"));
            if key.kind == KeyEventKind::Press {
                if let Some(binding) = chord
                    .as_deref()
                    .and_then(|chord| self.keymap.resolve(chord))
                {
                    self.pending_chord = None;
                    effects.extend(self.apply_decoded(
                        DecodedKey::Action {
                            action: binding.action,
                            select: false,
                        },
                        now_ms,
                        width,
                    ));
                    return effects;
                }
            }
            effects.extend(self.flush_chord());
        }
        if key.kind == KeyEventKind::Press {
            if let Some(chord) = event_chord(key) {
                if self.keymap.has_prefix(&chord)
                    || matches!(
                        decoded,
                        Some(DecodedKey::Action {
                            action: Action::CutPrefix,
                            ..
                        })
                    )
                {
                    self.last_interrupt = None;
                    self.last_activity = now_ms;
                    self.pending_chord = Some((chord.as_str().to_owned(), now_ms, decoded));
                    return effects;
                }
            }
        }
        if let Some(decoded) = decoded {
            effects.extend(self.apply_decoded(decoded, now_ms, width));
        }
        effects
    }

    fn apply_decoded(
        &mut self,
        decoded: DecodedKey,
        now_ms: u64,
        width: usize,
    ) -> Vec<InputEffect> {
        let mut effects = vec![];
        if matches!(
            decoded,
            DecodedKey::Action {
                action: Action::CutPrefix,
                ..
            }
        ) && self.editor.selection().is_empty()
        {
            return effects;
        }
        if !matches!(
            decoded,
            DecodedKey::Action {
                action: Action::CopyOrInterrupt,
                ..
            }
        ) {
            self.last_interrupt = None;
        }
        self.last_activity = now_ms;
        if self.search.is_some() {
            match &decoded {
                DecodedKey::Text(text) => {
                    effects.extend(self.committed_text(text.encode_utf8(&mut [0; 4]), now_ms));
                    return effects;
                }
                DecodedKey::Action {
                    action: Action::ReverseSearch,
                    ..
                } => {
                    self.search_previous();
                    return effects;
                }
                DecodedKey::Action {
                    action: Action::Escape,
                    ..
                } => {
                    self.finish_search(now_ms, false);
                    return effects;
                }
                DecodedKey::Action {
                    action: Action::Submit,
                    ..
                } => {
                    self.finish_search(now_ms, true);
                    return effects;
                }
                DecodedKey::Action {
                    action: Action::Delete(true),
                    ..
                } => {
                    if let Some(search) = self.search.as_mut() {
                        let end = search
                            .query
                            .grapheme_indices(true)
                            .last()
                            .map_or(0, |(i, _)| i);
                        search.query.truncate(end);
                        self.history.reset();
                    }
                    self.search_previous();
                    return effects;
                }
                _ => self.finish_search(now_ms, true),
            }
        }
        if let DecodedKey::Action { action, select } = &decoded {
            if self.transcript_search {
                match action {
                    Action::Submit | Action::Newline => {
                        return vec![InputEffect::FindStep {
                            previous: *action == Action::Newline,
                        }];
                    }
                    Action::Escape => return vec![InputEffect::Host(Action::Escape)],
                    _ => {}
                }
            }
            if !(*action == Action::Escape && self.editing.is_some())
                && self
                    .completion
                    .action(&mut self.editor, *action, *select, now_ms)
            {
                return effects;
            }
        }
        match decoded {
            DecodedKey::Text(text) => self.editor.insert(text.encode_utf8(&mut [0; 4]), now_ms),
            DecodedKey::Action { action, select } => match action {
                Action::Select(motion) => self.editor.move_cursor(motion, true, width),
                Action::Move(motion) => {
                    if self.dock_items
                        && self.editor.text().is_empty()
                        && !select
                        && matches!(motion, Motion::Up | Motion::Down)
                    {
                        effects.push(InputEffect::Host(if motion == Motion::Up {
                            Action::PreviousDock
                        } else {
                            Action::Tab
                        }));
                        return effects;
                    }
                    let previous = motion == Motion::Up;
                    let history = !select
                        && !self.transcript_search
                        && matches!(motion, Motion::Up | Motion::Down)
                        && self.editor.recall(&mut self.history, previous, now_ms);
                    if !history {
                        self.editor.move_cursor(motion, select, width);
                    }
                }
                Action::Delete(backwards) => self.editor.delete(backwards, now_ms),
                Action::DeleteWord(backwards) => self.editor.delete_word(backwards, now_ms),
                Action::Kill(motion) => self.editor.kill(motion, now_ms),
                Action::SelectAll => self.editor.select_all(),
                Action::Undo => {
                    self.editor.undo();
                }
                Action::Redo => {
                    self.editor.redo();
                }
                Action::Yank => self.editor.yank(now_ms),
                Action::RotateYank => self.editor.rotate_yank(now_ms),
                Action::Submit => {
                    if let Some(text) = self.editor.enter(false, now_ms) {
                        self.history.push(text.clone());
                        let edit = self.editing.take().map(|edit| {
                            self.editor = edit.draft;
                            self.history.restore_navigation(edit.navigation);
                            edit.message
                        });
                        effects.push(InputEffect::Submit {
                            text,
                            queued: self.busy,
                            edit,
                        });
                    }
                }
                Action::Newline => {
                    self.editor.enter(true, now_ms);
                }
                Action::LiteralTab => self.editor.insert("\t", now_ms),
                Action::Tab if self.dock_items => effects.push(InputEffect::Host(Action::Tab)),
                Action::Tab => {
                    let column = self
                        .editor
                        .layout(usize::MAX)
                        .position(self.editor.cursor())
                        .1;
                    let tab = self.editor.settings().tab_width;
                    self.editor.insert(&" ".repeat(tab - column % tab), now_ms);
                }
                Action::CopyOrInterrupt => {
                    if !self.editor.selection().is_empty() {
                        self.last_interrupt = None;
                        effects.push(InputEffect::Copy(self.editor.selected_text().to_owned()));
                        self.editor.break_group();
                    } else if let Some(markdown) = self
                        .transcript_selection
                        .as_ref()
                        .filter(|text| !text.is_empty())
                    {
                        self.last_interrupt = None;
                        self.editor.break_group();
                        effects.push(InputEffect::Copy(markdown.clone()));
                    } else if self.last_interrupt.is_some_and(|at| {
                        now_ms >= at
                            && now_ms - at <= self.editor.settings().double_interrupt_ms as u64
                    }) {
                        effects.push(InputEffect::Quit);
                    } else {
                        self.last_interrupt = Some(now_ms);
                        if self.busy {
                            effects.push(InputEffect::Interrupt);
                        } else if !self.editor.text().is_empty() {
                            self.editor.set_text("", now_ms);
                        } else {
                            effects.push(InputEffect::Interrupt);
                        }
                    }
                }
                Action::DeleteOrQuit => {
                    if self.editor.text().is_empty() {
                        effects.push(InputEffect::Quit);
                    } else {
                        self.editor.delete(false, now_ms);
                    }
                }
                Action::CutPrefix => {
                    let text = self.editor.selected_text().to_owned();
                    self.editor.replace_selection("", now_ms);
                    effects.push(InputEffect::Copy(text));
                }
                Action::Paste => {
                    if self.question {
                        self.comment_mode = true;
                    }
                    effects.push(InputEffect::Paste);
                }
                Action::ExternalEditor => effects.push(InputEffect::ExternalEditor),
                Action::ReverseSearch => {
                    let original = self.editor.clone();
                    let navigation = self.history.navigation.clone();
                    self.history.reset();
                    self.search = Some(Search {
                        original,
                        navigation,
                        query: String::new(),
                        matched: false,
                    });
                    self.search_previous();
                }
                Action::Escape if self.editing.is_some() => {
                    if let Some(edit) = self.editing.take() {
                        self.editor = edit.draft;
                        self.history.restore_navigation(edit.navigation);
                        self.completion.dismiss(&self.editor);
                    }
                }
                Action::Escape if !self.busy && !self.editor.selection().is_empty() => {
                    self.editor.set_cursor(self.editor.cursor(), false);
                }
                other => {
                    self.editor.break_group();
                    effects.push(InputEffect::Host(other));
                }
            },
        }
        effects
    }

    fn search_previous(&mut self) {
        if let Some(search) = self.search.as_mut() {
            if let Some(text) = self.history.reverse_search(&search.query) {
                let settings = *self.editor.settings();
                self.editor = Editor::new(&text);
                self.editor.set_settings(settings);
                search.matched = true;
            } else {
                search.matched = false;
            }
        }
    }

    fn finish_search(&mut self, now_ms: u64, accept: bool) {
        if let Some(search) = self.search.take() {
            let chosen = self.editor.text().to_owned();
            self.editor = search.original;
            self.history.restore_navigation(search.navigation);
            if accept && search.matched {
                self.editor.set_text(&chosen, now_ms);
                self.history.reset();
            }
        }
    }

    /// Mouse selection uses the same grapheme layout as movement and rendering.
    pub fn mouse(
        &mut self,
        event: MouseEvent,
        area: Rect,
        viewport: &mut Viewport,
        now_ms: u64,
    ) -> Vec<InputEffect> {
        if event.kind == MouseEventKind::Up(MouseButton::Left) {
            self.dragging = false;
            return vec![];
        }
        if area.width == 0 || area.height == 0 {
            return vec![];
        }
        let inside = area.contains((event.column, event.row).into());
        let down = event.kind == MouseEventKind::Down(MouseButton::Left);
        let drag = event.kind == MouseEventKind::Drag(MouseButton::Left) && self.dragging;
        if !drag && !(down && inside) {
            return vec![];
        }
        let effects = self.flush_chord();
        self.last_interrupt = None;
        self.finish_search(now_ms, true);
        let layout = self.editor.layout(usize::from(area.width));
        if drag && event.row < area.y {
            viewport.scroll = viewport.scroll.saturating_sub(1);
        }
        if drag && event.row >= area.bottom() {
            viewport.scroll = (viewport.scroll + 1).min(layout.rows.len().saturating_sub(1));
        }
        let row =
            usize::from(event.row.saturating_sub(area.y).min(area.height - 1)) + viewport.scroll;
        let column = usize::from(event.column.saturating_sub(area.x).min(area.width))
            + if self.editor.settings().wrap {
                0
            } else {
                viewport.horizontal
            };
        let offset = layout.hit_test(row, column);
        self.last_activity = now_ms;
        if down {
            let count = self
                .click
                .filter(|(x, y, at, _)| {
                    *x == event.column
                        && *y == event.row
                        && now_ms.saturating_sub(*at)
                            <= self.editor.settings().multi_click_ms as u64
                })
                .map_or(1, |(_, _, _, count)| count % 3 + 1);
            self.click = Some((event.column, event.row, now_ms, count));
            self.dragging = true;
            match count {
                _ if event.modifiers.contains(KeyModifiers::SHIFT) => {
                    self.editor.set_cursor(offset, true)
                }
                2 => self.editor.select_word(offset),
                3 => self.editor.select_line(offset),
                _ => self.editor.set_cursor(offset, false),
            }
        } else {
            self.editor.set_cursor(offset, true);
        }
        effects
    }
}
