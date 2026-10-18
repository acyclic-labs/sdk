use crate::buffer::slice;
use crate::key_event::KeyEvent;
use crate::{Action, DecodedKey, Editor, InputKeymap, Motion, decode_key};
use caseless::Caseless;
use std::{collections::BTreeMap, ops::Range, rc::Rc};
mod files;
use files::Entry;
use unicode_segmentation::UnicodeSegmentation;

fn prefix(value: &str, query: &str) -> bool {
    value
        .get(..query.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(query))
}

fn theme_prefix(value: &str, query: &str) -> bool {
    value.to_lowercase().starts_with(&query.to_lowercase())
}

/// Completion namespace derived from command and workspace files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionKind {
    /// Leading slash command or alias.
    Command,
    /// Theme name following /theme, from commands/themes.json.
    Theme,
    /// A whitespace-delimited file mention, including quoted paths.
    Mention,
}

/// Exact prompt token and prefix to complete. Ranges include the slash or @.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionRequest {
    /// File namespace.
    pub kind: CompletionKind,
    /// Whole token to replace, including any suffix after the cursor.
    pub range: Range<usize>,
    /// Prefix without a slash, @ or surrounding quote.
    pub query: String,
    /// Cursor at the time the token was identified.
    pub cursor: usize,
}

/// File candidate. The widget constructs the slash/@ syntax on acceptance.
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
    /// End acceptance after a separator (tools, /theme and completed file names).
    /// Existing horizontal whitespace is preserved and included in the edit.
    pub append_space: bool,
}

/// Identify the token under the cursor without triggering on emails or \@.
pub fn completion_request(editor: &Editor) -> Option<CompletionRequest> {
    request_from(editor, 0, &mut 0, None, None)
}

/// Parser facts about the text verified at `CompletionMenu::version`. Each
/// span runs from a trigger (or a leading slash) to just past the last byte its
/// parse examined, `usize::MAX` while that depends on text after the end; spans
/// are sorted and merged. A position below `scanned` and inside no span is a
/// checkpoint: no earlier token depends on bytes at or after it, so a parse
/// resumed there equals the full parse for every cursor at or after it.
#[derive(Clone, Debug, Default)]
struct Checkpoints {
    spans: Vec<Range<usize>>,
    scanned: usize,
}

impl Checkpoints {
    /// Keep only facts that no byte at or after `at` contributed to.
    fn edited(&mut self, at: usize) {
        let keep = self.spans.partition_point(|span| span.end <= at);
        let first = self.spans.get(keep).map_or(at, |span| span.start);
        self.scanned = self.scanned.min(first).min(at);
        self.spans.truncate(keep);
    }

    /// The latest checkpoint at or before `at <= scanned`.
    fn before(&self, at: usize) -> usize {
        match self.spans[..self.spans.partition_point(|span| span.start < at)].last() {
            Some(span) if span.end > at => span.start,
            _ => at,
        }
    }

    /// Forget facts from `from` onwards before a parse records them again.
    fn resume(&mut self, from: usize) -> &mut Self {
        self.spans
            .truncate(self.spans.partition_point(|span| span.start < from));
        self.scanned = from;
        self
    }

    fn record(&mut self, start: usize, end: usize) {
        match self.spans.last_mut() {
            Some(last) if start < last.end => last.end = last.end.max(end),
            _ => self.spans.push(start..end),
        }
    }
}

/// Just past the character at `at`, or `usize::MAX` when none is written yet.
fn past(text: &str, at: usize) -> usize {
    slice(text, at..)
        .chars()
        .next()
        .map_or(usize::MAX, |c| at + c.len_utf8())
}

/// A command name, or a theme argument, at the start of a `/` prompt.
fn command_request(
    text: &str,
    cursor: usize,
    work: &mut usize,
    themes: Option<&BTreeMap<String, usize>>,
    frontier: &mut usize,
) -> Option<CompletionRequest> {
    let end = scan_until(text, char::is_whitespace, work).unwrap_or(text.len());
    if cursor <= end {
        *frontier = usize::MAX;
        return Some(CompletionRequest {
            kind: CompletionKind::Command,
            range: 0..end,
            query: slice(text, 1..cursor).to_owned(),
            cursor,
        });
    }
    let name = slice(text, 1..end);
    let theme = themes.map_or_else(
        || name.eq_ignore_ascii_case("theme"),
        |names| names.contains_key(&name.to_ascii_lowercase()),
    );
    *frontier = past(text, end);
    if !theme {
        return None;
    }
    let rest = slice(text, end..);
    let start = end + rest.len() - rest.trim_start_matches([' ', '\t']).len();
    let line_end =
        end + scan_until(rest, |c| matches!(c, '\n' | '\r'), work).unwrap_or(text.len() - end);
    *frontier = past(text, line_end);
    (cursor >= start && cursor <= line_end).then(|| CompletionRequest {
        kind: CompletionKind::Theme,
        range: start..line_end,
        query: slice(text, start..cursor).to_owned(),
        cursor,
    })
}

fn request_from(
    editor: &Editor,
    from: usize,
    work: &mut usize,
    themes: Option<&BTreeMap<String, usize>>,
    mut marks: Option<&mut Checkpoints>,
) -> Option<CompletionRequest> {
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
    if let Some(marks) = marks.as_deref_mut() {
        marks.scanned = cursor;
    }
    let mut found = if from == 0 && text.starts_with('/') && cursor >= 1 {
        let mut frontier = usize::MAX;
        let found = command_request(text, cursor, work, themes, &mut frontier);
        if let Some(marks) = marks.as_deref_mut() {
            marks.record(0, frontier);
        }
        found
    } else {
        None
    };
    if found.is_none() {
        for (relative, character) in slice(text, from..)
            .char_indices()
            .take_while(|(at, _)| from + *at <= cursor)
        {
            *work += character.len_utf8();
            let start = from + relative;
            if character != '@' || start > 0 && !slice(text, ..start).ends_with(char::is_whitespace)
            {
                continue;
            }
            let tail = slice(text, start + 1..);
            let (query_start, query_end, end) = if let Some(quoted) = tail.strip_prefix('"') {
                let end =
                    scan_until(quoted, |c| matches!(c, '"' | '\n'), work).unwrap_or(quoted.len());
                let close = start + 2 + end;
                (
                    start + 2,
                    close,
                    close + usize::from(slice(text, close..).starts_with('"')),
                )
            } else {
                let end =
                    start + 1 + scan_until(tail, char::is_whitespace, work).unwrap_or(tail.len());
                (start + 1, end, end)
            };
            if tail.starts_with('"')
                && end > query_end
                && end < text.len()
                && !slice(text, end..).starts_with(char::is_whitespace)
            {
                let token_end = end
                    + scan_until(slice(text, end..), char::is_whitespace, work)
                        .unwrap_or(text.len() - end);
                if let Some(marks) = marks.as_deref_mut() {
                    marks.record(start, past(text, token_end));
                }
                if start <= cursor && cursor <= token_end {
                    return None;
                }
                continue;
            }
            if let Some(marks) = marks.as_deref_mut() {
                marks.record(start, past(text, end));
            }
            if cursor >= query_start && cursor <= end {
                found = Some(CompletionRequest {
                    kind: CompletionKind::Mention,
                    range: start..end,
                    query: slice(text, query_start..cursor.min(query_end)).to_owned(),
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
    let boundary = |at| editor.is_boundary(at);
    if !boundary(request.range.start) || !boundary(request.range.end) {
        return None;
    }
    Some(request)
}

/// Case-insensitive subsequence score for file paths. A lower score prefers
/// earlier and contiguous matches; a non-subsequence returns None.
pub fn fuzzy_score(query: &str, candidate: &str) -> Option<usize> {
    let mut query = query.chars().default_case_fold().peekable();
    if query.peek().is_none() {
        return Some(0);
    }
    let mut previous = 0;
    let mut score = 0usize;
    for (index, character) in candidate.chars().default_case_fold().enumerate() {
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

/// Popup file fold; acceptance checks the editor version to reject stale data.
#[derive(Clone, Debug, Default)]
pub struct CompletionMenu {
    request: Option<CompletionRequest>,
    items: BTreeMap<(bool, usize, u64, usize), Rc<Completion>>,
    files: BTreeMap<String, (u64, Vec<Entry>)>,
    theme_commands: BTreeMap<String, usize>,
    next_file: u64,
    version: Option<(Rc<()>, usize, usize)>,
    marks: Checkpoints,
    /// Cumulative bytes examined while locating tokens and candidates scored.
    pub work: CompletionWork,
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

    /// Refresh from editor deltas. File changes arrive through `set_file`.
    pub fn refresh(&mut self, editor: &Editor) {
        let version = editor.completion_version();
        if self
            .version
            .as_ref()
            .is_some_and(|old| same_version(old, &version))
        {
            return;
        }
        // Resume at the latest parser checkpoint before both the cursor and
        // the earliest byte changed since `version`. An unknown edit history
        // (or dismissal, catalog theme change) has no facts to reuse.
        let edit = self.version.as_ref().and_then(|old| {
            if Rc::ptr_eq(&old.0, &version.0) {
                return Some(usize::MAX);
            }
            let (base, range, _) = editor.completion_change()?;
            Rc::ptr_eq(base, &old.0).then_some(range.start)
        });
        self.marks.edited(edit.unwrap_or(0));
        let cursor = editor.cursor();
        let from = self.marks.before(cursor.min(self.marks.scanned));
        // Behind the verified region, query without discarding later facts.
        let marks = (cursor >= self.marks.scanned).then(|| self.marks.resume(from));
        let request = request_from(
            editor,
            from,
            &mut self.work.prompt_bytes,
            Some(&self.theme_commands),
            marks,
        );
        self.version = Some(version);
        if request != self.dismissed {
            self.dismissed = None;
        }
        if request == self.dismissed || request.is_none() {
            self.request = None;
            self.items.clear();
            self.selected = 0;
            return;
        }
        self.dismissed = None;
        if self.request == request {
            return;
        }
        let narrower = self
            .request
            .as_ref()
            .zip(request.as_ref())
            .is_some_and(|(old, new)| old.kind == new.kind && new.query.starts_with(&old.query));
        self.request = request;
        self.selected = 0;
        if narrower {
            let old = std::mem::take(&mut self.items);
            for ((_, _, order, index), item) in old {
                self.rank(order, index, item);
            }
        } else {
            self.items.clear();
            let entries: Vec<_> = self
                .files
                .iter()
                .flat_map(|(_, (order, entries))| {
                    entries
                        .iter()
                        .enumerate()
                        .map(|(index, entry)| (*order, index, entry.clone()))
                })
                .collect();
            for (order, index, entry) in entries {
                self.rank_entry(order, index, &entry);
            }
        }
    }

    fn rank_entry(&mut self, order: u64, index: usize, entry: &Entry) {
        if self.request.as_ref().is_some_and(|r| r.kind == entry.kind) {
            self.rank(order, index, entry.item.clone());
        }
    }

    fn rank(&mut self, order: u64, index: usize, item: Rc<Completion>) {
        let Some(request) = &self.request else {
            return;
        };
        self.work.candidates += 1;
        let value = item
            .value
            .strip_prefix('/')
            .filter(|_| request.kind == CompletionKind::Command)
            .unwrap_or(&item.value);
        if value.is_empty()
            || value.chars().any(char::is_control)
            || value.contains('"') && request.kind != CompletionKind::Theme
            || request.kind == CompletionKind::Command && value.chars().any(char::is_whitespace)
        {
            return;
        }
        let score = match request.kind {
            CompletionKind::Command => prefix(&item.value, &request.query).then_some(0),
            CompletionKind::Theme => theme_prefix(&item.value, &request.query).then_some(0),
            CompletionKind::Mention => fuzzy_score(&request.query, &item.value),
        };
        if let Some(score) = score {
            self.items.insert((!item.tool, score, order, index), item);
        }
    }

    /// Ranked candidates borrowed by the popup; unchanged frames allocate nothing.
    pub fn items(&self) -> impl ExactSizeIterator<Item = &Completion> {
        self.items.values().map(Rc::as_ref)
    }

    /// Whether results still belong to the current editor version.
    pub fn active(&self, editor: &Editor) -> bool {
        !self.items.is_empty()
            && self.request.is_some()
            && self
                .version
                .as_ref()
                .is_some_and(|old| same_version(old, &editor.completion_version()))
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
        self.dismissed = request_from(editor, 0, &mut 0, Some(&self.theme_commands), None);
        self.version = Some(editor.completion_version());
        self.marks.edited(0);
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
        let Some(item) = self.items().nth(self.selected) else {
            return false;
        };
        let Some(request) = self.request.as_ref() else {
            return false;
        };
        let value = match request.kind {
            CompletionKind::Command => item.value.strip_prefix('/').unwrap_or(&item.value),
            CompletionKind::Mention | CompletionKind::Theme => item.value.as_str(),
        };
        if value.is_empty()
            || value.chars().any(char::is_control)
            || value.contains('"') && request.kind != CompletionKind::Theme
        {
            return false;
        }
        let mut replacement = match request.kind {
            CompletionKind::Command if value.chars().any(char::is_whitespace) => return false,
            CompletionKind::Command => format!("/{value}"),
            CompletionKind::Theme => value.to_owned(),
            CompletionKind::Mention if value.chars().any(char::is_whitespace) => {
                format!("@\"{value}\"")
            }
            CompletionKind::Mention => format!("@{value}"),
        };
        let mut range = request.range.clone();
        if item.append_space
            || item.tool
            || request.kind == CompletionKind::Command && value == "theme"
        {
            // Reusing whitespace must place the caret after it. Include it in
            // this edit so redo records the final cursor too. A paste stays atomic.
            let folded = editor
                .collapsed_pastes()
                .iter()
                .any(|r| r.start == range.end);
            let separator = if folded {
                None
            } else {
                slice(editor.text(), range.end..)
                    .graphemes(true)
                    .next()
                    .filter(|s| s.starts_with(|c: char| c.is_whitespace() && c != '\n'))
            };
            if let Some(separator) = separator {
                replacement.push_str(separator);
                range.end += separator.len();
            } else {
                replacement.push(' ');
            }
        }
        if !editor.replace_range(range, &replacement, now_ms) {
            return false;
        }
        self.dismiss(editor);
        true
    }
}

fn same_version(a: &(Rc<()>, usize, usize), b: &(Rc<()>, usize, usize)) -> bool {
    Rc::ptr_eq(&a.0, &b.0) && a.1 == b.1 && a.2 == b.2
}

/// Observable completion work, independent of wall clock speed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CompletionWork {
    /// Prompt bytes examined when a token must be reidentified.
    pub prompt_bytes: usize,
    /// Candidate matching operations, excluding tree lookup work.
    pub candidates: usize,
}

fn scan_until(text: &str, mut stop: impl FnMut(char) -> bool, work: &mut usize) -> Option<usize> {
    text.char_indices().find_map(|(at, c)| {
        *work += c.len_utf8();
        stop(c).then_some(at)
    })
}

#[cfg(test)]
mod incremental_tests;
