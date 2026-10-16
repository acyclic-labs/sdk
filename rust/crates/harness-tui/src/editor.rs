use std::{cell::RefCell, ops::Range, rc::Rc};
use unicode_segmentation::UnicodeSegmentation;

mod completion_edit;

use crate::{
    Layout, PromptHistory,
    buffer::{Buffer, normalize},
    history::{Delta, Edit},
};

/// Cursor motion, independent of terminal key encodings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Motion {
    /// Previous grapheme or collapsed paste.
    Left,
    /// Next grapheme or collapsed paste.
    Right,
    /// Previous whitespace-delimited word.
    WordLeft,
    /// Next whitespace-delimited word.
    WordRight,
    /// Start of logical line.
    LineStart,
    /// End of logical line.
    LineEnd,
    /// Start of buffer.
    Start,
    /// End of buffer.
    End,
    /// Previous visual row, keeping the display column.
    Up,
    /// Next visual row, keeping the display column.
    Down,
    /// Previous logical line, keeping the display column.
    LogicalUp,
    /// Next logical line, keeping the display column.
    LogicalDown,
}

/// Pure prompt state. All offsets are UTF-8 bytes at grapheme boundaries.
#[derive(Clone, Debug, Default)]
pub struct Editor {
    buffer: Buffer,
    settings: crate::UiSettings,
    undo: Vec<Edit>,
    redo: Vec<Edit>,
    group_open: bool,
    sticky: Option<usize>,
    kills: Vec<String>,
    kill_active: bool,
    yank: Option<(Buffer, usize)>,
    layout: RefCell<(usize, Rc<Layout>)>,
}

impl Editor {
    /// Create an editor with the supplied text and its cursor at the end.
    pub fn new(text: &str) -> Self {
        let text = normalize(text);
        Self {
            buffer: Buffer::new(text),
            ..Self::default()
        }
    }

    /// Current view/editor preferences, folded from configuration files.
    pub fn settings(&self) -> &crate::UiSettings {
        &self.settings
    }

    /// Apply followed preferences without replacing the prompt or its undo history.
    pub fn set_settings(&mut self, settings: crate::UiSettings) -> bool {
        if !settings.valid() {
            return false;
        }
        if self.settings != settings {
            self.settings = settings;
            self.invalidate_layout();
            self.break_group();
            self.sticky = None;
        }
        true
    }

    /// Full text, including collapsed pastes.
    pub fn text(&self) -> &str {
        &self.buffer.text
    }
    /// Cursor byte offset.
    pub fn cursor(&self) -> usize {
        self.buffer.cursor
    }
    /// Selection anchor byte offset.
    pub fn anchor(&self) -> usize {
        self.buffer.anchor
    }
    /// Selection in document order.
    pub fn selection(&self) -> Range<usize> {
        self.buffer.selection()
    }
    /// Selected text, including the original contents of collapsed pastes.
    pub fn selected_text(&self) -> &str {
        &self.text()[self.selection()]
    }
    /// Collapsed paste ranges in document order.
    pub fn collapsed_pastes(&self) -> &[Range<usize>] {
        &self.buffer.folds
    }
    /// Heap bytes retained by undo and redo, including edit and fold metadata.
    /// Working text, wrapping and the kill ring are excluded.
    pub fn retained_undo_bytes(&self) -> usize {
        (self.undo.capacity() + self.redo.capacity()) * size_of::<Edit>()
            + self
                .undo
                .iter()
                .chain(&self.redo)
                .map(Edit::heap_bytes)
                .sum::<usize>()
    }

    /// Memoized display layout; changing selection does not rebuild wrapping.
    pub fn layout(&self, width: usize) -> Rc<Layout> {
        let width = if self.settings.wrap {
            width.max(1)
        } else {
            usize::MAX
        };
        if self.layout.borrow().0 != width {
            *self.layout.borrow_mut() = (width, Rc::new(Layout::new(self, width)));
        }
        Rc::clone(&self.layout.borrow().1)
    }

    fn invalidate_layout(&mut self) {
        self.layout.get_mut().0 = 0;
    }

    /// Place the cursor at or before a byte offset, optionally extending selection.
    pub fn set_cursor(&mut self, offset: usize, select: bool) {
        self.buffer.cursor = self.buffer.floor(offset);
        if !select {
            self.buffer.anchor = self.buffer.cursor;
        }
        self.break_group();
        self.sticky = None;
    }

    /// Select the entire buffer.
    pub fn select_all(&mut self) {
        self.buffer.anchor = 0;
        self.buffer.cursor = self.text().len();
        self.break_group();
    }

    /// Select the whitespace-delimited word at an offset.
    pub fn select_word(&mut self, offset: usize) {
        self.set_cursor(offset, false);
        let range = self.buffer.word_range(self.cursor());
        self.buffer.anchor = range.start;
        self.buffer.cursor = range.end;
    }

    /// Select a logical line, including its trailing newline if present.
    pub fn select_line(&mut self, offset: usize) {
        self.set_cursor(offset, false);
        self.buffer.anchor = self.line_start();
        let end = self.line_end();
        self.buffer.cursor = self.buffer.next(end);
    }

    fn line_start(&self) -> usize {
        let layout = self.layout(usize::MAX);
        layout.rows[layout.position(self.cursor()).0].start
    }

    fn line_end(&self) -> usize {
        let layout = self.layout(usize::MAX);
        layout.rows[layout.position(self.cursor()).0].end
    }

    /// Move in a display-width layout. Width zero is treated as one column.
    pub fn move_cursor(&mut self, motion: Motion, select: bool, width: usize) {
        let range = self.selection();
        if !select && !range.is_empty() && matches!(motion, Motion::Left | Motion::Right) {
            self.set_cursor(
                if motion == Motion::Left {
                    range.start
                } else {
                    range.end
                },
                false,
            );
            return;
        }
        let vertical = matches!(
            motion,
            Motion::Up | Motion::Down | Motion::LogicalUp | Motion::LogicalDown
        );
        let target = match motion {
            Motion::Left => self.buffer.previous(self.cursor()),
            Motion::Right => self.buffer.next(self.cursor()),
            Motion::WordLeft => self.buffer.word_left(self.cursor()),
            Motion::WordRight => self.buffer.word_right(self.cursor()),
            Motion::LineStart => self.buffer.floor(self.line_start()),
            Motion::LineEnd => self.buffer.floor(self.line_end()),
            Motion::Start => 0,
            Motion::End => self.text().len(),
            Motion::Up | Motion::Down | Motion::LogicalUp | Motion::LogicalDown => {
                let logical = matches!(motion, Motion::LogicalUp | Motion::LogicalDown);
                let layout = self.layout(if logical { usize::MAX } else { width });
                let (row, column) = layout.position(self.cursor());
                let column = *self.sticky.get_or_insert(column);
                let row = if matches!(motion, Motion::Up | Motion::LogicalUp) {
                    row.saturating_sub(1)
                } else {
                    (row + 1).min(layout.rows.len().saturating_sub(1))
                };
                layout.hit_test(row, column)
            }
        };
        self.buffer.cursor = self.buffer.floor(target);
        if !select {
            self.buffer.anchor = self.cursor();
        }
        self.break_group();
        if !vertical {
            self.sticky = None;
        }
    }

    /// Insert committed text, grouped by monotonic milliseconds, size and whitespace.
    pub fn insert(&mut self, text: &str, now_ms: u64) {
        let text = normalize(text);
        self.replace(self.selection(), &text, now_ms, 1, false, false);
    }

    /// Insert a paste as one undo step, collapsing past the byte or line threshold.
    pub fn paste(&mut self, text: &str, now_ms: u64) {
        let text = normalize(text);
        let fold = text.len() > self.settings.paste_collapse_bytes
            || text.lines().count() > self.settings.paste_collapse_lines;
        self.replace(self.selection(), &text, now_ms, 1, true, fold);
    }

    /// Replace the current selection with text in one undo step.
    pub fn replace_selection(&mut self, text: &str, now_ms: u64) {
        self.replace(self.selection(), &normalize(text), now_ms, 1, true, false);
    }

    /// Load a whole prompt or draft as one undo step, normalizing line endings.
    pub fn set_text(&mut self, text: &str, now_ms: u64) {
        self.replace(
            0..self.text().len(),
            &normalize(text),
            now_ms,
            1,
            true,
            false,
        );
    }

    /// Delete selection, or one grapheme backwards/forwards.
    pub fn delete(&mut self, backwards: bool, now_ms: u64) {
        let mut range = self.selection();
        if range.is_empty() {
            range = if backwards {
                self.buffer.previous(self.cursor())..self.cursor()
            } else {
                self.cursor()..self.buffer.next(self.cursor())
            };
        }
        self.replace(
            range,
            "",
            now_ms,
            if backwards { 2 } else { 3 },
            false,
            false,
        );
    }

    /// Delete to the previous or next word boundary.
    pub fn delete_word(&mut self, backwards: bool, now_ms: u64) {
        let mut range = self.selection();
        if range.is_empty() {
            range = if backwards {
                self.buffer.word_left(self.cursor())..self.cursor()
            } else {
                self.cursor()..self.buffer.word_right(self.cursor())
            };
        }
        self.replace(range, "", now_ms, 2, true, false);
    }

    /// Kill to line start/end or previous word. Consecutive kills accumulate.
    pub fn kill(&mut self, motion: Motion, now_ms: u64) {
        let backwards = matches!(motion, Motion::LineStart | Motion::WordLeft);
        let mut range = self.selection();
        if range.is_empty() {
            range = match motion {
                Motion::LineStart => self.buffer.floor(self.line_start())..self.cursor(),
                Motion::LineEnd => {
                    let end = self.line_end();
                    self.cursor()..if end == self.cursor() {
                        self.buffer.next(end)
                    } else {
                        self.buffer.floor(end)
                    }
                }
                Motion::WordLeft => self.buffer.word_left(self.cursor())..self.cursor(),
                _ => return,
            };
        }
        if range.is_empty() {
            return;
        }
        let killed = self.text()[range.clone()].to_owned();
        if self.kill_active {
            if let Some(last) = self.kills.last_mut() {
                if backwards {
                    last.insert_str(0, &killed);
                } else {
                    last.push_str(&killed);
                }
            }
        } else {
            self.kills.push(killed);
            if self.kills.len() > 32 {
                self.kills.remove(0);
            }
        }
        self.replace(range, "", now_ms, 2, true, false);
        self.kill_active = true;
    }

    /// Insert the most recently killed text.
    pub fn yank(&mut self, now_ms: u64) {
        if let Some(text) = self.kills.last().cloned() {
            let before = self.buffer.clone();
            self.replace(self.selection(), &text, now_ms, 1, true, false);
            self.yank = Some((before, self.kills.len() - 1));
        }
    }

    /// Rotate a preceding yank through the kill ring; unrelated edits cancel it.
    pub fn rotate_yank(&mut self, now_ms: u64) {
        if let Some((original, index)) = self.yank.clone() {
            let index = if index == 0 {
                self.kills.len() - 1
            } else {
                index - 1
            };
            let text = self.kills[index].clone();
            let before = self.buffer.clone();
            self.buffer = original.clone();
            self.buffer.replace(original.selection(), &text, false);
            self.record(
                Delta::between(&before, &self.buffer),
                now_ms,
                1,
                text.graphemes(true).count(),
                true,
            );
            self.yank = Some((original, index));
        }
    }

    /// Stop undo grouping (e.g. on focus changes or host actions).
    pub fn break_group(&mut self) {
        self.group_open = false;
        self.kill_active = false;
        self.yank = None;
    }

    fn replace(
        &mut self,
        range: Range<usize>,
        text: &str,
        at: u64,
        kind: u8,
        atomic: bool,
        fold: bool,
    ) {
        if range.is_empty() && text.is_empty() {
            return;
        }
        let size =
            text.graphemes(true).count() + self.text()[range.clone()].graphemes(true).count();
        let boundary = atomic
            || text.chars().any(char::is_whitespace)
            || self.text()[range.clone()].chars().any(char::is_whitespace);
        let mut delta = Delta::capture(&self.buffer, range.clone(), text);
        self.buffer.replace(range, text, fold);
        delta.finish(&self.buffer);
        self.record(delta, at, kind, size, boundary);
    }

    fn record(&mut self, delta: Delta, at: u64, kind: u8, size: usize, boundary: bool) {
        self.invalidate_layout();
        let grouped = self.group_open
            && self.undo.last().is_some_and(|edit| {
                edit.kind == kind
                    && !boundary
                    && !edit.boundary
                    && edit.size + size <= self.settings.undo_group_graphemes
                    && at >= edit.at
                    && at - edit.at <= self.settings.undo_group_ms as u64
                    && edit
                        .changes
                        .last()
                        .is_some_and(|last| last.after == delta.before)
            });
        if grouped {
            if let Some(edit) = self.undo.last_mut() {
                edit.changes.push(delta);
                edit.at = at;
                edit.size += size;
            }
        } else {
            self.undo.push(Edit {
                changes: vec![delta],
                at,
                kind,
                size,
                boundary,
            });
        }
        self.redo = vec![];
        self.break_group();
        self.group_open = !boundary;
        self.sticky = None;
    }

    /// Undo a whole edit group, restoring selection and collapsed paste state.
    pub fn undo(&mut self) -> bool {
        self.break_group();
        self.sticky = None;
        if let Some(edit) = self.undo.pop() {
            if self.undo.is_empty() {
                self.undo = vec![];
            }
            for delta in edit.changes.iter().rev() {
                delta.apply(&mut self.buffer, true);
            }
            self.invalidate_layout();
            self.redo.push(edit);
            true
        } else {
            false
        }
    }

    /// Redo a whole edit group.
    pub fn redo(&mut self) -> bool {
        self.break_group();
        self.sticky = None;
        if let Some(edit) = self.redo.pop() {
            if self.redo.is_empty() {
                self.redo = vec![];
            }
            for delta in &edit.changes {
                delta.apply(&mut self.buffer, false);
            }
            self.invalidate_layout();
            self.undo.push(edit);
            true
        } else {
            false
        }
    }

    /// Recall history only at the corresponding buffer edge.
    pub fn recall(&mut self, history: &mut PromptHistory, previous: bool, now_ms: u64) -> bool {
        if !self.selection().is_empty()
            || self.cursor() != if previous { 0 } else { self.text().len() }
        {
            return false;
        }
        let text = if previous {
            history.previous(&self.buffer.text)
        } else {
            history.next_entry()
        };
        if let Some(text) = text {
            let text = normalize(&text);
            if self.text().is_empty() && text.is_empty() {
                return true;
            }
            let range = 0..self.text().len();
            let size = self.text().graphemes(true).count() + text.graphemes(true).count();
            let mut delta = Delta::capture(&self.buffer, range.clone(), &text);
            self.buffer.replace(range, &text, false);
            // Keep previous-history navigation at the start for repeated Up.
            if previous {
                self.buffer.cursor = 0;
                self.buffer.anchor = 0;
            }
            delta.finish(&self.buffer);
            self.record(delta, now_ms, 1, size, true);
            true
        } else {
            false
        }
    }

    /// Enter returns a submitted prompt, or inserts a newline when modified.
    /// The caller appends submissions to the shared input history.
    /// A trailing backslash continues only at buffer end with no selection.
    pub fn enter(&mut self, modified: bool, now_ms: u64) -> Option<String> {
        if modified {
            self.replace_selection("\n", now_ms);
            return None;
        }
        if self.selection().is_empty()
            && self.cursor() == self.text().len()
            && self.text().ends_with('\\')
        {
            self.replace(
                self.cursor() - 1..self.cursor(),
                "\n",
                now_ms,
                1,
                true,
                false,
            );
            return None;
        }
        if self.text().trim().is_empty() {
            return None;
        }
        let text = self.text().to_owned();
        self.replace(0..self.text().len(), "", now_ms, 2, true, false);
        Some(text)
    }
}
