use super::ClientRenderer;
use crate::{
    client::{CommandFileRoot, CommandSession},
    tui::{self, TranscriptAnchor},
};
use acyclic_stream::StreamProvider;

fn find(layout: &serde_json::Value) -> bool {
    layout.get("open_view").and_then(serde_json::Value::as_str) == Some("find")
        && layout.get("view_input").and_then(serde_json::Value::as_str) == Some("find")
}

pub(super) fn reasoning<P: StreamProvider + ?Sized>(
    files: &CommandSession<P>,
) -> crate::Result<bool> {
    let settings = files.state_ref(CommandFileRoot::Config, "settings.json")?;
    let layout = files.state_ref(CommandFileRoot::Session, "layout.json")?;
    if layout.is_some_and(find) {
        return Ok(false);
    }
    settings
        .as_ref()
        .and_then(|value| value.get("reasoning"))
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| crate::Error::Invalid("reasoning must be a boolean".into()))
        })
        .transpose()
        .map(|value| value.unwrap_or(false))
}

impl ClientRenderer {
    pub(super) fn source_anchor(&self, mut anchor: TranscriptAnchor) -> TranscriptAnchor {
        anchor.origin = if anchor.part == tui::TranscriptPart::Answer {
            let prefix = self.archive.rows().len();
            if anchor.position.line < prefix {
                self.archive.origin(anchor.position.line)
            } else {
                self.task.origin(anchor.part, anchor.position.line - prefix)
            }
        } else {
            self.task.origin(anchor.part, anchor.position.line)
        };
        anchor
    }

    fn resolve_anchor(&self, mut anchor: TranscriptAnchor) -> TranscriptAnchor {
        if let Some(origin) = anchor.origin {
            anchor.position.line = if anchor.part == tui::TranscriptPart::Answer {
                self.archive.resolve(origin).or_else(|| {
                    self.task
                        .resolve(anchor.part, origin)
                        .map(|line| self.archive.rows().len() + line)
                })
            } else {
                self.task.resolve(anchor.part, origin)
            }
            .unwrap_or(0);
        }
        anchor
    }

    pub(super) fn select_view<P: StreamProvider + ?Sized>(
        &mut self,
        files: &CommandSession<P>,
        input: &tui::PromptInput,
        height: usize,
    ) -> crate::Result<()> {
        self.view_height = height;
        if height == 0 {
            self.search_open = false;
            self.close_search();
            self.wrapped_reasoning.hide();
            self.wrapped_transcript.hide();
            self.wrapped_tools.hide();
            self.top = crate::client::tui_anchor(files)?;
            return Ok(());
        }
        let layout = files
            .state_ref(CommandFileRoot::Session, "layout.json")?
            .unwrap_or(&serde_json::Value::Null);
        self.search_open = find(layout);
        if !self.search_open {
            self.close_search();
        }
        let rows = tui::TranscriptRows::new(&self.wrapped_transcript, &self.wrapped_tools);
        self.search.project(rows, self.search_open);
        let mut top = crate::client::tui_anchor(files)?.map(|anchor| self.resolve_anchor(anchor));
        if self.search_open {
            let index = layout
                .get("search_index")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            self.search.update(
                rows,
                layout
                    .get("search_query")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(""),
                index,
                input.theme.style(tui::config::ThemeRole::Selection),
            );
            top = self.search.selected_anchor(rows, index).or(top);
        }
        self.top = tui::view_transcript(
            [
                self.reasoning_visible
                    .then_some(&mut self.wrapped_reasoning),
                Some(&mut self.wrapped_transcript),
                Some(&mut self.wrapped_tools),
            ],
            top,
            height,
            usize::from(self.busy),
        );
        self.top = self.top.map(|anchor| self.source_anchor(anchor));
        if self.search_open {
            self.search.prepare(
                tui::TranscriptRows::new(&self.wrapped_transcript, &self.wrapped_tools),
                0,
                height,
            );
        }
        Ok(())
    }

    fn close_search(&mut self) {
        self.search.project(
            tui::TranscriptRows::new(&self.wrapped_transcript, &self.wrapped_tools),
            false,
        );
        self.wrapped_transcript.clear_search_index();
        self.wrapped_tools.clear_search_index();
    }

    /// First displayed source identity and its distance from a presented row.
    /// A distant seek has no retained distance and repaints normally.
    pub fn presented_position(
        &self,
        from: Option<TranscriptAnchor>,
    ) -> (Option<TranscriptAnchor>, Option<isize>) {
        for (part, source) in [
            (tui::TranscriptPart::Reasoning, &self.wrapped_reasoning),
            (tui::TranscriptPart::Answer, &self.wrapped_transcript),
            (tui::TranscriptPart::Tools, &self.wrapped_tools),
        ] {
            if source.shown().is_empty() {
                continue;
            }
            let top = self.source_anchor(TranscriptAnchor {
                origin: None,
                part,
                position: source.window().top,
            });
            let distance = from
                .filter(|from| from.part == part)
                .map(|from| self.resolve_anchor(from))
                .and_then(|from| source.cached_distance(from.position, top.position));
            return (Some(top), distance);
        }
        (None, None)
    }

    /// Move a source anchor through the current physical layout. Files remain
    /// authoritative; the caller publishes the returned anchor as a file change.
    pub fn move_view(
        &mut self,
        from: Option<TranscriptAnchor>,
        rows: isize,
    ) -> Option<TranscriptAnchor> {
        let from = from.map(|anchor| self.resolve_anchor(anchor));
        let anchor = tui::move_transcript(
            [
                self.reasoning_visible
                    .then_some(&mut self.wrapped_reasoning),
                Some(&mut self.wrapped_transcript),
                Some(&mut self.wrapped_tools),
            ],
            from,
            rows,
            self.view_height,
            usize::from(self.busy),
        );
        anchor.map(|anchor| self.source_anchor(anchor))
    }
}

impl ClientRenderer {
    /// Resolve a source-track position without traversing unrelated archive rows.
    pub fn anchor_at(&mut self, mut byte: usize) -> Option<TranscriptAnchor> {
        let mut anchor = None;
        for (part, source) in [
            (tui::TranscriptPart::Reasoning, &self.wrapped_reasoning),
            (tui::TranscriptPart::Answer, &self.wrapped_transcript),
            (tui::TranscriptPart::Tools, &self.wrapped_tools),
        ] {
            if source.source_len() == 0
                || part == tui::TranscriptPart::Reasoning && !self.reasoning_visible
            {
                continue;
            }
            let length = source.text().len() + 1;
            if byte < length {
                anchor = Some(TranscriptAnchor {
                    origin: None,
                    part,
                    position: source.position_of(byte.min(length - 1)),
                });
                break;
            }
            byte -= length;
        }
        let anchor = tui::view_transcript(
            [
                self.reasoning_visible
                    .then_some(&mut self.wrapped_reasoning),
                Some(&mut self.wrapped_transcript),
                Some(&mut self.wrapped_tools),
            ],
            anchor,
            self.view_height,
            usize::from(self.busy),
        );
        anchor.map(|anchor| self.source_anchor(anchor))
    }

    pub(super) fn source_window(&self) -> (std::ops::Range<usize>, usize) {
        let mut total = 0;
        let mut visible = None;
        for source in [
            self.reasoning_visible.then_some(&self.wrapped_reasoning),
            Some(&self.wrapped_transcript),
            Some(&self.wrapped_tools),
        ]
        .into_iter()
        .flatten()
        {
            if source.source_len() == 0 {
                continue;
            }
            let window = source.window();
            if window.end > window.start {
                let first = source.ranges().get(window.start);
                let last = source.ranges().get(window.end - 1);
                if let Some((first, last)) = first.zip(last) {
                    let range = visible
                        .get_or_insert(total + first.start..total + last.end.max(first.start + 1));
                    range.end = total + last.end.max(last.start + 1);
                }
            }
            total += source.text().len() + 1;
        }
        total += usize::from(self.busy);
        let mut visible = visible.unwrap_or(total..total);
        if self.top.is_none() {
            visible.end = total;
        }
        (visible, total)
    }
}
