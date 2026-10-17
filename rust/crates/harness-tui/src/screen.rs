use crate::{CompletionPopup, PromptBox, PromptInput, Viewport, widget::cells};
use ratatui::{
    Frame,
    layout::Rect,
    text::Line,
    widgets::{Block, Borders, Paragraph},
};

#[cfg(test)]
thread_local! { static ROW_WORK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

/// Borrowed presentation of followed files and client views. The caller owns
/// the live query; rendering never reads files or mutates durable UI state.
#[derive(Default)]
pub struct Screen<'a> {
    /// Agent breadcrumb and lifecycle indicator.
    pub header: Line<'a>,
    /// Already rendered transcript rows, including the current streaming tail.
    pub transcript: &'a [Line<'a>],
    /// Independently cached tool tail, following the answer rows without copies.
    pub tools: &'a [Line<'a>],
    /// Search decoration over the existing wrapping projection; only visible rows are painted.
    pub transcript_search: Option<(crate::TranscriptRows<'a>, &'a crate::TranscriptSearch)>,
    /// Visible current reasoning, folded from the same agent stream.
    pub reasoning: &'a [Line<'a>],
    /// Optional embedded terminal rows supplied by its terminal widget.
    pub terminal: &'a [Line<'a>],
    /// Diff rows supplied by the markdown/diff renderer.
    pub diff: &'a [Line<'a>],
    /// GOAL.md live-query projection.
    pub goal: &'a [Line<'a>],
    /// plans/ live-query projection.
    pub plans: &'a [Line<'a>],
    /// Notification/feedback rows from the followed attention files.
    pub notifications: &'a [Line<'a>],
    /// Optional modal view, rendered over the transcript while preserving it.
    pub overlay: &'a [Line<'a>],
    /// Catalog-owned title of the selected modal view.
    pub overlay_title: &'a str,
    /// The followed layout selects the local feedback input form.
    pub feedback: bool,
    /// Search selection overrides follow-tail while its file-backed form is open.
    pub search_row: Option<usize>,
    /// Status hints and model/usage summary.
    pub status: Line<'a>,
}

/// Areas shared by rendering and mouse routing, in absolute terminal cells.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScreenAreas {
    /// Whether the painted activity label needs another animation frame.
    pub animation: bool,
    /// Agent breadcrumb.
    pub header: Rect,
    /// Transcript viewport.
    pub transcript: Rect,
    /// Optional terminal panel.
    pub terminal: Rect,
    /// Optional right-hand file/diff views.
    pub side: Rect,
    /// Attention and feedback.
    pub notifications: Rect,
    /// Prompt including border.
    pub prompt: Rect,
    /// Completion overlay, immediately above the prompt.
    pub completions: Rect,
    /// Status hints.
    pub status: Rect,
}

fn take_top(area: &mut Rect, height: u16) -> Rect {
    let height = height.min(area.height);
    let result = Rect::new(area.x, area.y, area.width, height);
    area.y += height;
    area.height -= height;
    result
}

impl Screen<'_> {
    fn transcript_len(&self) -> usize {
        self.transcript_search
            .map_or(self.transcript.len() + self.tools.len(), |(rows, _)| {
                rows.len()
            })
    }

    /// Default follow-tail position; a followed layout file may override it.
    pub fn tail_scroll(&self, area: Rect, input: &PromptInput) -> usize {
        let height = self.areas(area, input).transcript.height;
        if self.feedback {
            0
        } else if self.overlay.is_empty() {
            (self.reasoning.len() + self.transcript_len() + usize::from(input.busy))
                .saturating_sub(usize::from(height))
        } else {
            self.overlay
                .len()
                .saturating_sub(usize::from(height.saturating_sub(2)))
        }
    }

    /// Compute one layout for both painting and input routing. Prompt growth is
    /// bounded by half the screen; optional panels yield space at small sizes.
    pub fn areas(&self, area: Rect, input: &PromptInput) -> ScreenAreas {
        let mut remaining = area;
        let header = take_top(&mut remaining, 1);
        let status_height = remaining.height.min(1);
        let status = Rect::new(
            area.x,
            remaining.bottom() - status_height,
            area.width,
            status_height,
        );
        remaining.height -= status_height;
        let prompt_height = PromptBox::new(input, 0)
            .height(area.width, area.height)
            .min(remaining.height);
        let prompt = Rect::new(
            area.x,
            remaining.bottom() - prompt_height,
            area.width,
            prompt_height,
        );
        remaining.height -= prompt_height;
        let notice_height = cells(
            self.notifications
                .len()
                .min(2)
                .min(usize::from(remaining.height)),
        );
        let notifications = Rect::new(
            area.x,
            remaining.bottom() - notice_height,
            area.width,
            notice_height,
        );
        remaining.height -= notice_height;
        let terminal_height = if self.terminal.is_empty() {
            0
        } else {
            remaining.height / 3
        };
        let terminal = Rect::new(
            area.x,
            remaining.bottom() - terminal_height,
            area.width,
            terminal_height,
        );
        remaining.height -= terminal_height;
        let side_width = if area.width >= 80
            && (!self.diff.is_empty() || !self.goal.is_empty() || !self.plans.is_empty())
        {
            area.width / 3
        } else {
            0
        };
        let side = Rect::new(
            remaining.right() - side_width,
            remaining.y,
            side_width,
            remaining.height,
        );
        remaining.width -= side_width;
        let completions = if input.completion.active(&input.editor) {
            CompletionPopup::new(&input.completion, &input.editor).area(prompt, area)
        } else {
            Rect::new(prompt.x, prompt.y, 0, 0)
        };
        ScreenAreas {
            animation: false,
            header,
            transcript: remaining,
            terminal,
            side,
            notifications,
            prompt,
            completions,
            status,
        }
    }

    /// Construct activity spans only when their row is inside the painted viewport.
    pub fn thinking_line(
        &self,
        input: &PromptInput,
        areas: &ScreenAreas,
        scroll: usize,
        now_ms: u64,
    ) -> Option<Line<'static>> {
        let area = areas.transcript;
        let row = self.reasoning.len() + self.transcript_len();
        if !input.busy
            || self.feedback
            || !self.overlay.is_empty()
            || area.width == 0
            || row < scroll
            || row - scroll >= usize::from(area.height)
        {
            return None;
        }
        let label = Rect::new(
            area.x,
            area.y + cells(row - scroll),
            area.width.min(crate::Thinking::WIDTH),
            1,
        );
        if areas.completions.intersection(label) == label {
            return None;
        }
        Some(
            crate::Thinking::new(&input.theme, now_ms)
                .reduced_motion(input.editor.settings().reduced_motion)
                .line(),
        )
    }

    /// Compose shared widgets without cloning client views or their transcript.
    /// Scroll and open panels are supplied from the user's followed layout file.
    pub fn render(
        &self,
        frame: &mut Frame<'_>,
        input: &PromptInput,
        viewport: &mut Viewport,
        transcript_scroll: usize,
        now_ms: u64,
    ) -> ScreenAreas {
        let mut areas = self.areas(frame.area(), input);
        let transcript_scroll = self.search_row.map_or(transcript_scroll, |row| {
            row.min(
                self.transcript_len()
                    .saturating_sub(usize::from(areas.transcript.height)),
            )
        });
        frame.render_widget(Paragraph::new(self.header.clone()), areas.header);
        let thinking = self.thinking_line(input, &areas, transcript_scroll, now_ms);
        areas.animation = thinking.is_some() && !input.editor.settings().reduced_motion;
        if self.feedback {
            frame.render_widget(crate::Feedback(&input.theme), areas.transcript);
        } else if self.overlay.is_empty() {
            let count = self.reasoning.len() + self.transcript_len() + usize::from(input.busy);
            let end =
                count.min(transcript_scroll.saturating_add(usize::from(areas.transcript.height)));
            for (row, index) in (transcript_scroll..end).enumerate() {
                #[cfg(test)]
                ROW_WORK.with(|work| work.set(work.get() + 1));
                let area = Rect::new(
                    areas.transcript.x,
                    areas.transcript.y + cells(row),
                    areas.transcript.width,
                    1,
                );
                if let Some(line) = self.reasoning.get(index) {
                    frame.render_widget(line, area);
                } else if index < self.reasoning.len() + self.transcript_len() {
                    let index = index - self.reasoning.len();
                    if let Some((projection, search)) = self.transcript_search {
                        frame.render_widget(search.row(projection, index), area);
                    } else if let Some(line) = self.transcript.get(index) {
                        frame.render_widget(line, area);
                    } else if let Some(line) = self.tools.get(index - self.transcript.len()) {
                        frame.render_widget(line, area);
                    }
                } else if let Some(line) = thinking.as_ref() {
                    frame.render_widget(line, area);
                }
            }
        } else {
            let block = Block::default()
                .borders(Borders::ALL)
                .title(self.overlay_title);
            rows(
                frame,
                self.overlay,
                block.inner(areas.transcript),
                transcript_scroll,
            );
            frame.render_widget(block, areas.transcript);
        }
        rows(frame, self.terminal, areas.terminal, 0);
        rows(frame, self.notifications, areas.notifications, 0);
        frame.render_widget(Paragraph::new(self.status.clone()), areas.status);
        let mut side = areas.side;
        let panels = [
            ("Diff", self.diff),
            ("GOAL.md", self.goal),
            ("plans/", self.plans),
        ];
        let count = panels.iter().filter(|(_, lines)| !lines.is_empty()).count();
        for (index, (title, lines)) in panels
            .into_iter()
            .filter(|(_, lines)| !lines.is_empty())
            .enumerate()
        {
            let height = side.height / cells(count - index);
            let panel = take_top(&mut side, height);
            let block = Block::default().borders(Borders::ALL).title(title);
            rows(frame, lines, block.inner(panel), 0);
            frame.render_widget(block, panel);
        }
        frame.render_stateful_widget(PromptBox::new(input, now_ms), areas.prompt, viewport);
        if let Some((x, y)) = viewport.cursor {
            frame.set_cursor_position((x, y));
        }
        if input.completion.active(&input.editor) {
            frame.render_widget(
                CompletionPopup::new(&input.completion, &input.editor),
                areas.completions,
            );
        }
        areas
    }
}
fn rows(frame: &mut Frame<'_>, lines: &[Line<'_>], area: Rect, scroll: usize) {
    for (row, line) in lines
        .iter()
        .skip(scroll)
        .take(usize::from(area.height))
        .enumerate()
    {
        frame.render_widget(line, Rect::new(area.x, area.y + cells(row), area.width, 1));
    }
}

#[cfg(test)]
#[path = "screen/tests.rs"]
mod tests;
