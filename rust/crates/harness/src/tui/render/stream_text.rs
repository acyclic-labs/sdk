use super::{Line, line};
use crate::{
    client::{TaskQueryView, ToolQueryView},
    task_volume::{Channel, FileKey, Query},
};
use acyclic_markdown::Markdown;
use std::{collections::VecDeque, sync::Arc};

struct SealedTurn {
    turn: u64,
    first: FileKey,
    prefix: Option<(FileKey, usize, acyclic_stream::OwnedBytes)>,
    end: usize,
}

/// Disposable parsing cache; all source bytes remain in ClientViews.
#[derive(Default)]
pub(super) struct StreamText {
    markdown: Markdown,
    append: Option<(usize, usize, usize)>,
    append_only: bool,
    stable: Vec<Line<'static>>,
    sealed: VecDeque<SealedTurn>,
    current_first: Option<FileKey>,
    current_prefix: Option<(FileKey, usize, acyclic_stream::OwnedBytes)>,
    dropped: usize,
    pending: [u8; 4],
    pending_len: usize,
    provisional_row: Option<usize>,
    newline: bool,
    source: Option<Arc<Query>>,
    first: Option<FileKey>,
    prefix: Option<(FileKey, usize, acyclic_stream::OwnedBytes)>,
    last: Option<FileKey>,
    segments: usize,
    turn: Option<u64>,
    tool_limit: usize,
    tool_lines: usize,
    tool_partial: bool,
    #[cfg(test)]
    pub(super) work: usize,
}
impl StreamText {
    pub(super) fn has_text(&self) -> bool {
        self.first.is_some()
    }
    fn offset(&self) -> usize {
        self.stable.len()
            + usize::from(
                !self.stable.is_empty()
                    && (!self.markdown.lines().is_empty() || self.pending_len != 0),
            )
    }
    pub(super) fn len(&self) -> usize {
        self.offset() + self.markdown.lines().len()
    }
    fn valid(&mut self, text: &str) -> usize {
        #[cfg(test)]
        {
            self.work += text.len();
        }
        if !text.is_empty() {
            self.newline = text.ends_with('\n');
        }
        let before = self.len();
        let changed = self.markdown.push(text);
        if !text.is_empty() && self.append_only {
            if let Some((row, span, byte)) = self.markdown.last_append() {
                let at = (self.offset() + row, span, byte);
                if self
                    .append
                    .is_none_or(|previous| previous.0 == at.0 && previous.1 == at.1)
                {
                    self.append.get_or_insert(at);
                } else {
                    self.append_only = false;
                    self.append = None;
                }
            } else {
                self.append_only = false;
                self.append = None;
            }
        }
        before.min(self.offset() + changed)
    }
    fn decode(&mut self, mut bytes: &[u8]) -> usize {
        let mut changed = self.len();
        while !bytes.is_empty() {
            match std::str::from_utf8(bytes) {
                Ok(text) => {
                    changed = changed.min(self.valid(text));
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    changed = changed.min(
                        self.valid(std::str::from_utf8(&bytes[..valid]).expect("valid prefix")),
                    );
                    bytes = &bytes[valid..];
                    if let Some(length) = error.error_len() {
                        changed = changed.min(self.valid("\u{fffd}"));
                        bytes = &bytes[length..];
                    } else {
                        self.pending[..bytes.len()].copy_from_slice(bytes);
                        self.pending_len = bytes.len();
                        break;
                    }
                }
            }
        }
        changed
    }
    pub(super) fn push(&mut self, mut bytes: &[u8]) -> usize {
        if self.pending_len != 0 || self.provisional_row.is_some() {
            self.append_only = false;
            self.append = None;
        }
        let mut changed = self.provisional_row.take().unwrap_or_else(|| self.len());
        while self.pending_len != 0 && !bytes.is_empty() {
            let mut boundary = [0; 4];
            let pending = self.pending_len;
            let take = (4 - pending).min(bytes.len());
            boundary[..pending].copy_from_slice(&self.pending[..pending]);
            boundary[pending..pending + take].copy_from_slice(&bytes[..take]);
            self.pending_len = 0;
            changed = changed.min(self.decode(&boundary[..pending + take]));
            bytes = &bytes[take..];
        }
        changed = changed.min(self.decode(bytes));
        if self.pending_len != 0 && !self.newline {
            changed = changed.min(self.len().saturating_sub(1));
        }
        changed
    }
    fn prefix_valid(&self, task: &TaskQueryView<'_>) -> bool {
        self.first
            .as_ref()
            .is_none_or(|key| task.file(key).is_some())
            && self.prefix.as_ref().is_none_or(|(key, index, bytes)| {
                task.file(key)
                    .and_then(|file| file.segments.get(*index))
                    .is_some_and(|current| {
                        current.as_ptr() == bytes.as_ptr() && current.len() == bytes.len()
                    })
            })
    }
    // Completed turns have independent Markdown state. Discard their rows and
    // checkpoints without feeding any retained bytes back through the parser.
    // A partial turn eviction can change fences, lists or reference definitions
    // throughout that turn and must be parsed from its new authoritative start.
    fn discard_turns(&mut self, task: &TaskQueryView<'_>, channel: Channel) -> bool {
        let mut through = 0;
        while let Some(front) = self.sealed.front() {
            let retained = task
                .text_from(Some(&front.first))
                .take_while(|text| text.turn == front.turn)
                .any(|text| text.channel == channel);
            if retained {
                break;
            }
            if let Some(front) = self.sealed.pop_front() {
                through = front.end;
            }
        }
        if through == 0 {
            return false;
        }
        self.dropped = self.discard_rows(through);
        for sealed in &mut self.sealed {
            sealed.end = sealed.end.saturating_sub(self.dropped);
        }
        if let Some(front) = self.sealed.front() {
            self.first = Some(front.first.clone());
            self.prefix = front.prefix.clone();
        } else {
            self.first = self.current_first.clone();
            self.prefix = self.current_prefix.clone();
        }
        self.prefix_valid(task)
    }
    pub(super) fn discard_rows(&mut self, through: usize) -> usize {
        if through == 0 {
            return 0;
        }
        let separator = usize::from(through < self.len());
        let removed = through + separator;
        self.stable.drain(..removed.min(self.stable.len()));
        self.provisional_row = self.provisional_row.map(|row| row.saturating_sub(removed));
        removed
    }
    pub(super) fn dropped(&self) -> usize {
        self.dropped
    }
    pub(super) fn update(&mut self, task: &TaskQueryView<'_>, channel: Channel) -> usize {
        self.dropped = 0;
        let same_source = self
            .source
            .as_ref()
            .is_some_and(|source| Arc::ptr_eq(source, task.source));
        if !same_source || (!self.prefix_valid(task) && !self.discard_turns(task, channel)) {
            *self = Self {
                source: Some(task.source.clone()),
                ..Self::default()
            };
        }
        self.append = None;
        self.append_only = self.provisional_row.is_none() && self.pending_len == 0;
        let mut changed = self.provisional_row.take().unwrap_or_else(|| self.len());
        let from = self.last.clone();
        for text in task
            .text_from(from.as_ref())
            .filter(|text| text.channel == channel)
        {
            let start = if self.last.as_ref() == Some(&text.file.key) {
                self.segments
            } else {
                0
            };
            if start > text.file.segments.len() {
                *self = Self::default();
                return self.update(task, channel);
            }
            if self.turn != Some(text.turn) && self.first.is_some() {
                changed = changed.min(self.boundary());
                if let (Some(turn), Some(first)) = (self.turn, self.current_first.take()) {
                    self.sealed.push_back(SealedTurn {
                        turn,
                        first,
                        prefix: self.current_prefix.take(),
                        end: self.stable.len(),
                    });
                }
            }
            self.turn = Some(text.turn);
            self.current_first
                .get_or_insert_with(|| text.file.key.clone());
            for (index, segment) in text.file.segments.iter().enumerate().skip(start) {
                if self.prefix.is_none() && !segment.is_empty() {
                    self.prefix = Some((text.file.key.clone(), index, segment.clone()));
                }
                if self.current_prefix.is_none() && !segment.is_empty() {
                    self.current_prefix = Some((text.file.key.clone(), index, segment.clone()));
                }
                changed = changed.min(self.push(segment));
            }
            self.first.get_or_insert_with(|| text.file.key.clone());
            self.last = Some(text.file.key.clone());
            self.segments = text.file.segments.len();
        }
        changed
    }
    pub(super) fn tool_lines(&self) -> usize {
        self.tool_lines + usize::from(self.tool_partial)
    }
    fn push_tool(&mut self, bytes: &[u8]) -> usize {
        let mut end = 0;
        while end < bytes.len() && self.tool_lines < self.tool_limit {
            let byte = bytes[end];
            end += 1;
            if byte == b'\n' {
                self.tool_lines += 1;
                self.tool_partial = false;
            } else {
                self.tool_partial = true;
            }
        }
        self.push(&bytes[..end])
    }
    pub(super) fn update_tool(
        &mut self,
        tool: &ToolQueryView<'_>,
        stderr: bool,
        limit: usize,
    ) -> usize {
        if self.tool_limit != limit
            || !self
                .source
                .as_ref()
                .is_some_and(|source| Arc::ptr_eq(source, tool.source()))
            || self
                .first
                .as_ref()
                .is_some_and(|key| tool.file(key).is_none())
            || self.prefix.as_ref().is_some_and(|(key, index, bytes)| {
                tool.file(key)
                    .and_then(|file| file.segments.get(*index))
                    .is_none_or(|current| {
                        current.as_ptr() != bytes.as_ptr() || current.len() != bytes.len()
                    })
            })
        {
            *self = Self {
                source: Some(tool.source().clone()),
                tool_limit: limit,
                ..Self::default()
            };
        }
        let mut changed = self.provisional_row.take().unwrap_or_else(|| self.len());
        let after = self.last.clone();
        for file in tool.output_from(stderr, after.as_ref()) {
            let start = if self.last.as_ref() == Some(&file.key) {
                self.segments
            } else {
                0
            };
            if start > file.segments.len() {
                *self = Self::default();
                return self.update_tool(tool, stderr, limit);
            }
            for (index, bytes) in file.segments.iter().enumerate().skip(start) {
                if self.prefix.is_none() && !bytes.is_empty() {
                    self.prefix = Some((file.key.clone(), index, bytes.clone()));
                }
                changed = changed.min(self.push_tool(bytes));
            }
            self.first.get_or_insert_with(|| file.key.clone());
            self.last = Some(file.key.clone());
            self.segments = file.segments.len();
        }
        changed
    }
    /// Markdown blocks cannot extend across distinct model turns or outcomes.
    pub(super) fn boundary(&mut self) -> usize {
        self.append = None;
        self.append_only = false;
        let changed = self.provisional_row.take().unwrap_or_else(|| self.len());
        if self.pending_len != 0 {
            self.pending_len = 0;
            self.valid("\u{fffd}");
        }
        if !self.markdown.lines().is_empty() {
            if !self.stable.is_empty() {
                self.stable.push(Line::default());
            }
            let finished = std::mem::take(&mut self.markdown);
            self.stable
                .extend(finished.into_lines().into_iter().map(|row| {
                    Line::from(
                        row.spans
                            .into_iter()
                            .map(|span| {
                                let text = match super::super::display_text(&span.text) {
                                    std::borrow::Cow::Borrowed(_) => span.text,
                                    std::borrow::Cow::Owned(text) => text,
                                };
                                super::Span::styled(text, super::style(span.style))
                            })
                            .collect::<Vec<_>>(),
                    )
                }));
        }
        self.markdown = Markdown::default();
        self.newline = false;
        changed
    }
    pub(super) fn literal(&mut self, rows: impl IntoIterator<Item = Line<'static>>) -> usize {
        let changed = self.boundary();
        if !self.stable.is_empty() {
            self.stable.push(Line::default());
        }
        self.stable.extend(rows);
        changed
    }
    /// Borrow only the appended display suffix; replacement paths return None.
    pub(super) fn append_delta(
        &self,
    ) -> Option<(usize, usize, std::borrow::Cow<'_, str>, super::Style)> {
        if !self.append_only || self.pending_len != 0 {
            return None;
        }
        let (row, span, byte) = self.append?;
        let source = self
            .markdown
            .lines()
            .get(row.checked_sub(self.offset())?)?
            .spans
            .get(span)?;
        let text = super::super::display_text(source.text.get(byte..)?);
        Some((row, span, text, super::style(source.style)))
    }

    pub(super) fn apply_rows(&mut self, target: &mut Vec<Line<'static>>, changed: usize) {
        target.drain(..self.dropped.min(target.len()));
        if let Some((row, span, text, _)) = self.append_delta()
            && target.len() == self.len()
            && row + 1 == target.len()
            && let Some(line) = target.get_mut(row)
            && span + 1 == line.spans.len()
            && let Some(last) = line.spans.get_mut(span)
        {
            last.content.to_mut().push_str(&text);
            return;
        }
        target.truncate(changed);
        target.extend(self.rows(changed));
    }

    pub(super) fn rows(&mut self, changed: usize) -> Vec<Line<'static>> {
        let mut rows: Vec<_> = self.stable.iter().skip(changed).cloned().collect();
        let offset = self.offset();
        if offset > self.stable.len() && changed <= self.stable.len() {
            rows.push(Line::default());
        }
        rows.extend(
            self.markdown
                .lines()
                .iter()
                .skip(changed.saturating_sub(offset))
                .map(line),
        );
        if self.pending_len != 0 {
            if rows.is_empty() || self.newline {
                rows.push(Line::default());
            }
            self.provisional_row = Some(changed + rows.len() - 1);
            let last = rows.last_mut().expect("provisional row");
            let style = last
                .spans
                .last()
                .map_or(super::Style::default(), |span| span.style);
            last.spans.push(super::Span::styled("\u{fffd}", style));
        }
        rows
    }
}

#[cfg(test)]
mod tests;
