use crate::key_event::KeyEvent;
use crate::{Action, DecodedKey, Editor, InputKeymap, Motion, decode_key};
use std::ops::Range;
use unicode_segmentation::UnicodeSegmentation;

/// Completion namespace, supplied to each application provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionKind {
    /// Leading slash command or alias.
    Command,
    /// A whitespace-delimited file mention, including quoted paths.
    Mention,
}

/// Exact prompt token and prefix to complete. Ranges include the slash or @.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionRequest {
    /// Provider namespace.
    pub kind: CompletionKind,
    /// Whole token to replace, including any suffix after the cursor.
    pub range: Range<usize>,
    /// Prefix without a slash, @ or surrounding quote.
    pub query: String,
    /// Cursor at the time the provider was queried.
    pub cursor: usize,
}

/// Provider result. The widget constructs the slash/@ syntax on acceptance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completion {
    /// Display label, independent of insertion text.
    pub label: String,
    /// Command name or raw relative file path, without mention syntax.
    pub value: String,
    /// Optional one-line description.
    pub description: String,
    /// Direct tool commands sort before other results and show a [tool] suffix.
    pub tool: bool,
    /// Add a space after acceptance (tools, /theme and completed file names).
    pub append_space: bool,
}

/// Renderer-independent boundary over command and file live-query snapshots.
/// Command providers filter names/aliases by prefix. Mention results are fuzzy
/// filtered and ranked by the widget; providers may narrow their file snapshots.
pub trait CompletionProvider {
    /// Return candidates for a snapshot of the active token.
    fn complete(&self, request: &CompletionRequest) -> Vec<Completion>;
}

/// Identify the token under the cursor without triggering on emails or \@.
pub fn completion_request(editor: &Editor) -> Option<CompletionRequest> {
    if !editor.selection().is_empty() {
        return None;
    }
    let text = editor.text();
    let cursor = editor.cursor();
    if editor
        .collapsed_pastes()
        .iter()
        .any(|range| range.start < cursor && cursor <= range.end)
    {
        return None;
    }
    let mut found = None;
    if text.starts_with('/') && cursor >= 1 {
        let end = text.find(char::is_whitespace).unwrap_or(text.len());
        if cursor <= end {
            found = Some(CompletionRequest {
                kind: CompletionKind::Command,
                range: 0..end,
                query: text[1..cursor].to_owned(),
                cursor,
            });
        }
    }
    if found.is_none() {
        for (start, character) in text.char_indices().take_while(|(at, _)| *at <= cursor) {
            if character != '@' || start > 0 && !text[..start].ends_with(char::is_whitespace) {
                continue;
            }
            let tail = &text[start + 1..];
            let (query_start, query_end, end) = if let Some(quoted) = tail.strip_prefix('"') {
                let end = quoted.find(['"', '\n']).unwrap_or(quoted.len());
                let close = start + 2 + end;
                (
                    start + 2,
                    close,
                    close + usize::from(text[close..].starts_with('"')),
                )
            } else {
                let end = start + 1 + tail.find(char::is_whitespace).unwrap_or(tail.len());
                (start + 1, end, end)
            };
            if tail.starts_with('"')
                && end > query_end
                && end < text.len()
                && !text[end..].starts_with(char::is_whitespace)
            {
                let token_end = end
                    + text[end..]
                        .find(char::is_whitespace)
                        .unwrap_or(text.len() - end);
                if start <= cursor && cursor <= token_end {
                    return None;
                }
                continue;
            }
            if cursor >= query_start && cursor <= end {
                found = Some(CompletionRequest {
                    kind: CompletionKind::Mention,
                    range: start..end,
                    query: text[query_start..cursor.min(query_end)].to_owned(),
                    cursor,
                });
                break;
            }
        }
    }
    let request = found?;
    if editor
        .collapsed_pastes()
        .iter()
        .any(|r| r.start < request.range.end && r.end > request.range.start)
    {
        return None;
    }
    let boundary =
        |at| at == text.len() || text.grapheme_indices(true).any(|(index, _)| index == at);
    if !boundary(request.range.start) || !boundary(request.range.end) {
        return None;
    }
    Some(request)
}

/// Case-insensitive subsequence score for file paths. A lower score prefers
/// earlier and contiguous matches; a non-subsequence returns None.
pub fn fuzzy_score(query: &str, candidate: &str) -> Option<usize> {
    let mut query = query.chars().flat_map(char::to_lowercase).peekable();
    if query.peek().is_none() {
        return Some(0);
    }
    let mut previous = 0;
    let mut score = 0usize;
    for (index, character) in candidate.chars().flat_map(char::to_lowercase).enumerate() {
        if query.peek() == Some(&character) {
            score = score.saturating_add(index.saturating_sub(previous));
            previous = index + 1;
            query.next();
            if query.peek().is_none() {
                return Some(score);
            }
        }
    }
    None
}

/// Popup state; acceptance checks the current token again to reject stale data.
#[derive(Clone, Debug, Default)]
pub struct CompletionMenu {
    request: Option<CompletionRequest>,
    /// Provider results, with tools first.
    pub items: Vec<Completion>,
    /// Selected row, wrapping during navigation.
    pub selected: usize,
    dismissed: Option<CompletionRequest>,
}

impl CompletionMenu {
    /// Handle popup keys using the editor's effective keymap. Returns true when
    /// consumed, so accepting with Enter never also submits the prompt.
    pub fn key(
        &mut self,
        editor: &mut Editor,
        key: KeyEvent,
        bindings: &InputKeymap,
        now_ms: u64,
    ) -> bool {
        match decode_key(key, bindings) {
            Some(DecodedKey::Action { action, select }) => {
                self.action(editor, action, select, now_ms)
            }
            _ => false,
        }
    }

    pub(crate) fn action(
        &mut self,
        editor: &mut Editor,
        action: Action,
        select: bool,
        now_ms: u64,
    ) -> bool {
        if !self.active(editor) {
            return false;
        }
        match action {
            Action::Tab | Action::Submit => {
                self.accept(editor, now_ms);
            }
            Action::PreviousDock => self.cycle(true),
            Action::Move(Motion::Up) if !select => self.cycle(true),
            Action::Move(Motion::Down) if !select => self.cycle(false),
            Action::Escape => self.dismiss(editor),
            _ => return false,
        }
        editor.break_group();
        true
    }

    /// Refresh after edits, using the application's command and mention providers.
    pub fn refresh(&mut self, editor: &Editor, providers: &[&dyn CompletionProvider]) {
        let request = completion_request(editor);
        if request == self.dismissed || request.is_none() {
            self.request = None;
            self.items.clear();
            self.selected = 0;
            return;
        }
        self.dismissed = None;
        if let Some(request) = request {
            let selected = (self.request.as_ref() == Some(&request))
                .then(|| self.items.get(self.selected))
                .flatten()
                .map(|item| item.value.clone());
            let mut ranked: Vec<_> = providers
                .iter()
                .flat_map(|provider| provider.complete(&request))
                .filter_map(|item| {
                    let score = match request.kind {
                        CompletionKind::Mention => fuzzy_score(&request.query, &item.value)?,
                        CompletionKind::Command => 0,
                    };
                    Some((score, item))
                })
                .collect();
            ranked.sort_by_key(|(score, item)| (!item.tool, *score));
            self.items = ranked.into_iter().map(|(_, item)| item).collect();
            self.selected = selected
                .and_then(|value| self.items.iter().position(|item| item.value == value))
                .unwrap_or(0);
            self.request = Some(request);
        }
    }

    /// Whether results still belong to the current token.
    pub fn active(&self, editor: &Editor) -> bool {
        !self.items.is_empty()
            && self.request.is_some()
            && self.request == completion_request(editor)
    }

    /// Cycle candidates in either direction.
    pub fn cycle(&mut self, previous: bool) {
        if self.items.is_empty() {
            self.selected = 0;
            return;
        }
        let current = self.selected % self.items.len();
        self.selected = if previous {
            (current + self.items.len() - 1) % self.items.len()
        } else {
            (current + 1) % self.items.len()
        };
    }

    /// Close the popup until the current token changes.
    pub fn dismiss(&mut self, editor: &Editor) {
        self.dismissed = completion_request(editor);
        self.request = None;
        self.items.clear();
        self.selected = 0;
    }

    /// Apply one item as an undo step, retaining @ and safely quoting file names.
    pub fn accept(&mut self, editor: &mut Editor, now_ms: u64) -> bool {
        if !self.active(editor) {
            self.dismiss(editor);
            return false;
        }
        let Some(item) = self.items.get(self.selected) else {
            return false;
        };
        let Some(request) = self.request.as_ref() else {
            return false;
        };
        let value = match request.kind {
            CompletionKind::Command => item.value.strip_prefix('/').unwrap_or(&item.value),
            CompletionKind::Mention => item.value.as_str(),
        };
        if value.is_empty() || value.chars().any(char::is_control) || value.contains('"') {
            return false;
        }
        let mut replacement = match request.kind {
            CompletionKind::Command if value.chars().any(char::is_whitespace) => return false,
            CompletionKind::Command => format!("/{value}"),
            CompletionKind::Mention if value.chars().any(char::is_whitespace) => {
                format!("@\"{value}\"")
            }
            CompletionKind::Mention => format!("@{value}"),
        };
        if (item.append_space
            || item.tool
            || request.kind == CompletionKind::Command && value == "theme")
            && !editor.text()[request.range.end..].starts_with(char::is_whitespace)
        {
            replacement.push(' ');
        }
        if !editor.replace_range(request.range.clone(), &replacement, now_ms) {
            return false;
        }
        self.dismiss(editor);
        true
    }
}
