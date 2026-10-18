use super::{Line, line};
use crate::{
    client::{TaskQueryView, ToolQueryView},
    task_volume::{Channel, FileKey, Query},
};
use acyclic_markdown::Markdown;
use std::sync::Arc;

/// Disposable parsing cache; all source bytes remain in ClientViews.
#[derive(Default)]
pub(super) struct StreamText {
    markdown: Markdown,
    stable: Vec<Line<'static>>,
    pending: [u8; 4],
    pending_len: usize,
    provisional_row: Option<usize>,
    newline: bool,
    source: Option<Arc<Query>>,
    first: Option<FileKey>,
    prefix: Option<(FileKey, usize, acyclic_stream::OwnedBytes)>,
    last: Option<FileKey>,
    segments: usize,
    cut: u64,
    turn: Option<(crate::OperationId, u64)>,
    tool_limit: usize,
    tool_lines: usize,
    tool_partial: bool,
    #[cfg(test)]
    pub(super) work: usize,
}
impl StreamText {
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
    pub(super) fn update(&mut self, task: &TaskQueryView<'_>, channel: Channel) -> usize {
        if !self
            .source
            .as_ref()
            .is_some_and(|source| Arc::ptr_eq(source, task.source))
            || self
                .last
                .as_ref()
                .is_some_and(|last| task.text_changed_before(last, self.cut, channel))
            || self
                .first
                .as_ref()
                .is_some_and(|key| task.file(key).is_none())
            || self.prefix.as_ref().is_some_and(|(key, index, bytes)| {
                task.file(key)
                    .and_then(|file| file.segments.get(*index))
                    .is_none_or(|current| {
                        current.as_ptr() != bytes.as_ptr() || current.len() != bytes.len()
                    })
            })
        {
            *self = Self {
                source: Some(task.source.clone()),
                ..Self::default()
            };
        }
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
            if self.turn != Some((text.execution, text.turn)) && self.first.is_some() {
                changed = changed.min(self.boundary());
            }
            self.turn = Some((text.execution, text.turn));
            for (index, segment) in text.file.segments.iter().enumerate().skip(start) {
                if self.prefix.is_none() && !segment.is_empty() {
                    self.prefix = Some((text.file.key.clone(), index, segment.clone()));
                }
                changed = changed.min(self.push(segment));
            }
            self.first.get_or_insert_with(|| text.file.key.clone());
            self.last = Some(text.file.key.clone());
            self.segments = text.file.segments.len();
        }
        if self.turn.is_some_and(|(_, turn)| {
            task.file(&FileKey::Task {
                task: task.task.clone(),
                file: crate::task_volume::TaskFile::Completion(turn),
            })
            .is_some()
        }) {
            changed = changed.min(self.boundary());
        }
        self.cut = task.cursor.position;
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
        let changed = self.provisional_row.take().unwrap_or_else(|| self.len());
        if self.pending_len != 0 {
            self.pending_len = 0;
            self.valid("\u{fffd}");
        }
        if !self.markdown.lines().is_empty() {
            if !self.stable.is_empty() {
                self.stable.push(Line::default());
            }
            self.stable.extend(self.markdown.lines().iter().map(line));
        }
        self.markdown = Markdown::default();
        self.newline = false;
        changed
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
