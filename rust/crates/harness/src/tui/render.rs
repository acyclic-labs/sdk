use super::{
    Screen,
    ratatui::{
        style::{Color, Modifier, Style},
        text::{Line, Span},
    },
};
use crate::{client::ClientViews, wire};
use acyclic_markdown::Markdown;
use std::sync::OnceLock;

mod approvals;
mod archive;
#[cfg(not(target_arch = "wasm32"))]
mod pacing;
mod selection;
mod source_rows;
mod stream_text;
mod task;
mod tool;
mod viewport;
use stream_text::StreamText;

/// Disposable incremental render cache. Authoritative state is always ClientViews
/// and the followed files; this cache is never serialized or written back.
#[derive(Default)]
pub struct ClientRenderer {
    #[cfg(not(target_arch = "wasm32"))]
    pacing: pacing::Pacer,
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    file_cut: Option<(u64, wire::Authority)>,
    #[cfg(test)]
    folded_views: usize,
    key: String,
    help: super::help::Help,
    search: super::TranscriptSearch,
    search_open: bool,
    busy: bool,
    tool_details: bool,
    task: task::TaskRows,
    archive: archive::Archive,
    transcript: OnceLock<Vec<Line<'static>>>,
    approval_rows: Vec<Line<'static>>,
    wrapped: bool,
    top: Option<super::TranscriptAnchor>,
    view_height: usize,
    reasoning_visible: bool,
    wrapped_transcript: super::WrappedLines,
    wrapped_tools: super::WrappedLines,
    wrapped_reasoning: super::WrappedLines,
    wrapped_goal: super::WrappedLines,
    wrapped_plans: super::WrappedLines,
    side_wrapped: bool,
    goal_changed: usize,
    plans_changed: usize,
    goal_source: Vec<crate::task_volume::HeadFile>,
    goal: Vec<Line<'static>>,
    plan_source: Vec<crate::task_volume::HeadFile>,
    plans: Vec<Line<'static>>,
    header: Line<'static>,
    status: Line<'static>,
    notices: Vec<Line<'static>>,
    notice_file: Option<crate::task_volume::FileValue>,
    file_notices: Vec<Line<'static>>,
    outcome_cut: Option<(u64, u64)>,
    outcome_notice: Option<Line<'static>>,
    confirmation_notice: Option<&'static str>,
    composed_notices: Vec<Line<'static>>,
    diff: Vec<Line<'static>>,
}

impl ClientRenderer {
    /// Keep optional diff failures inside the pane without ending the session.
    pub fn update_diff(&mut self, rows: crate::Result<Vec<Line<'static>>>) {
        self.diff = rows.unwrap_or_else(|error| {
            vec![Line::styled(
                super::display_text(&format!("Diff unavailable: {error}")).into_owned(),
                Style::default().fg(Color::Red),
            )]
        });
    }
    #[cfg(test)]
    pub(crate) fn text_work(&self) -> usize {
        self.task.work() + self.archive.work()
    }
    /// Reuse the accepted file projection across input, scroll and timer frames.
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    pub fn update_files(
        &mut self,
        client: &crate::client::TaskFileClient,
        authority: &wire::Authority,
    ) {
        let revision = client.presentation_revision();
        if self
            .file_cut
            .as_ref()
            .is_some_and(|(cut, owner)| *cut == revision && owner == authority)
        {
            return;
        }
        self.update(client.views(), authority);
        self.file_cut = Some((revision, authority.clone()));
    }

    /// Update only the active agent. Stable transcript rows survive text deltas;
    /// the Markdown parser supplies the first changed open-block row.
    pub fn update(&mut self, views: &ClientViews, authority: &wire::Authority) {
        self.select_authority(authority);
        let selected = selection::selected::<true>(views, authority);
        let archive = views
            .selections()
            .find_map(|(_, selection)| match selection.query() {
                crate::task_volume::Query::CommandSession { session, .. } => {
                    Some((selection, session.clone()))
                }
                _ => None,
            });
        let old_prefix = self.archive.rows().len();
        let archive_changed = self
            .archive
            .update(archive.as_ref().map(|(files, session)| {
                (*files, crate::client::OutputOwner::Session(session.clone()))
            }));
        let owner = match &archive {
            None => task::AnswerOwner::Task,
            Some((files, session)) => {
                let archived = selected.as_ref().is_some_and(|(_, task)| {
                    crate::TaskId::parse(task.as_str()).is_ok_and(|task| {
                        files.has_command_text(
                            &crate::client::OutputOwner::Session(session.clone()),
                            task,
                            self.archive.after(),
                        )
                    })
                });
                if archived {
                    task::AnswerOwner::Archived
                } else {
                    task::AnswerOwner::Session
                }
            }
        };
        let task_changed = self.task.update(
            selected.as_ref().map(|(files, task)| files.task(task)),
            self.tool_details,
            owner,
        );
        let approval_rows = if self.task.busy() {
            approvals::rows(selected.as_ref().map(|(files, task)| files.task(task)))
        } else {
            Vec::new()
        };
        if self.approval_rows != approval_rows {
            self.approval_rows = approval_rows;
            self.transcript.take();
        }
        self.project_rows(old_prefix, archive_changed, task_changed);
        self.documents(views);
        self.busy = self.task.busy();
        self.header = Line::from(vec![
            Span::raw(if self.busy { "● " } else { "○ " }),
            Span::styled("GraphCoder", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(if self.task.stopped() {
                " ■ STOPPED"
            } else {
                ""
            }),
        ]);
        self.status = Line::raw(super::display_text(&self.task.usage()).into_owned());
        self.notices = self.task.notices();
        self.compose_notices();
    }

    fn select_authority(&mut self, authority: &wire::Authority) {
        let key = authority.id.clone();
        if self.key != key {
            *self = Self {
                key,
                tool_details: self.tool_details,
                #[cfg(not(target_arch = "wasm32"))]
                pacing: std::mem::take(&mut self.pacing),
                ..Self::default()
            };
        }
        #[cfg(test)]
        {
            self.folded_views += 1;
        }
        self.wrapped = false;
        self.side_wrapped = false;
    }

    fn project_rows(&mut self, old_prefix: usize, archive_changed: usize, task_changed: usize) {
        let prefix = self.archive.rows().len();
        let archive_dropped = self.archive.dropped();
        if archive_changed > 0 && archive_dropped > 0 && archive_dropped <= old_prefix {
            self.wrapped_transcript
                .replace_range(0..archive_dropped, &[]);
        }
        let old_prefix = old_prefix.saturating_sub(if archive_changed > 0 {
            archive_dropped
        } else {
            0
        });
        if archive_changed >= old_prefix && prefix == old_prefix && task_changed > 0 {
            let dropped = self.task.answer_dropped();
            if dropped > 0 && prefix + dropped <= self.wrapped_transcript.source_len() {
                self.wrapped_transcript
                    .replace_range(prefix..prefix + dropped, &[]);
            }
        }
        let changed = if archive_changed < old_prefix || prefix != old_prefix {
            archive_changed
        } else {
            prefix + task_changed
        };
        let answer_total = prefix + self.task.rows().len();
        let total = answer_total + self.approval_rows.len();
        let changed = if self.transcript.get().is_none() {
            changed.min(answer_total)
        } else {
            changed
        };
        if archive_dropped > 0
            || self.task.answer_dropped() > 0
            || changed < total
            || self.wrapped_transcript.source_len() != total
        {
            self.transcript.take();
        }
        if self.approval_rows.is_empty()
            && archive_changed >= old_prefix
            && prefix == old_prefix
            && self.wrapped_transcript.source_len() == total
            && changed + 1 == total
            && let Some((text, style)) = self.task.answer_append()
        {
            self.wrapped_transcript.append_line(&text, style);
        } else {
            self.wrapped_transcript.replace_parts(
                &[self.archive.rows(), self.task.rows(), &self.approval_rows],
                changed,
            );
        }
        if self.task.tools_full() {
            self.wrapped_tools.replace(self.task.tools(), 0);
        } else {
            for (old, new) in self.task.tool_edits() {
                self.wrapped_tools.replace_range(
                    old.clone(),
                    self.task.tools().get(new.clone()).unwrap_or_default(),
                );
            }
        }
        let (changed, rows) = self.task.reasoning();
        let dropped = self.task.reasoning_dropped();
        if changed > 0 && dropped > 0 && dropped <= self.wrapped_reasoning.source_len() {
            self.wrapped_reasoning.replace_range(0..dropped, &[]);
        }
        if changed + 1 == rows.len()
            && self.wrapped_reasoning.source_len() == rows.len()
            && let Some((text, style)) = self.task.reasoning_append()
        {
            self.wrapped_reasoning.append_line(&text, style);
        } else {
            self.wrapped_reasoning.replace(rows, changed);
        }
    }

    fn documents(&mut self, views: &ClientViews) {
        let goals = || {
            views
                .selections()
                .filter_map(|(_, files)| files.files().get(&crate::task_volume::FileKey::Goal))
        };
        let plans = || views.selections().flat_map(|(_, files)| files.plan_files());
        if !self.goal_source.iter().eq(goals()) {
            self.goal_source = goals().cloned().collect();
            let mut parser = Markdown::default();
            for file in &self.goal_source {
                for bytes in &file.segments {
                    parser.push(&String::from_utf8_lossy(bytes));
                }
            }
            self.goal = parser.lines().iter().map(line).collect();
            self.goal_changed = 0;
        }
        if !self.plan_source.iter().eq(plans()) {
            self.plan_source = plans().cloned().collect();
            let mut parser = Markdown::default();
            for (index, file) in self.plan_source.iter().enumerate() {
                let crate::task_volume::FileKey::Plan(name) = &file.key else {
                    unreachable!()
                };
                if index != 0 {
                    parser.push("\n\n");
                }
                parser.push("# ");
                parser.push(name.as_str());
                parser.push("\n");
                for bytes in &file.segments {
                    parser.push(&String::from_utf8_lossy(bytes));
                }
            }
            self.plans = parser.lines().iter().map(line).collect();
            self.plans_changed = 0;
        }
    }

    /// Apply followed tool visibility, rebuilding the disposable transcript only
    /// when this preference changes. The file remains the authoritative setting.
    pub fn configure<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        views: &ClientViews,
        authority: &wire::Authority,
        files: &crate::client::CommandSession<P>,
    ) -> crate::Result<()> {
        let settings = files.state_ref(crate::client::CommandFileRoot::Config, "settings.json")?;
        let details = settings
            .as_ref()
            .and_then(|value| value.get("tool_details"))
            .map(|value| {
                value
                    .as_bool()
                    .ok_or_else(|| crate::Error::Invalid("tool_details must be a boolean".into()))
            })
            .transpose()?
            .unwrap_or(false);
        // Layout is followed by CommandSession, outside the answer selection.
        let after = files.history_after()?;
        if self.tool_details != details || self.archive.after() != after {
            self.tool_details = details;
            self.task = task::TaskRows::default();
            self.transcript.take();
            self.wrapped_transcript.replace(&[], 0);
            self.archive = archive::Archive::new(after);
            self.update(views, authority);
        }
        self.busy = files.pending_execution()? || self.task.busy();
        if let Some(marker) = self.header.spans.first_mut() {
            marker.content = std::borrow::Cow::Borrowed(if self.busy { "● " } else { "○ " });
        }
        self.compose_notices();
        Ok(())
    }

    /// Project changed transcript/reasoning rows at the composed viewport width.
    /// Repeated frames at the same width do not reflow the unchanged prefix.
    pub fn layout<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        files: &crate::client::CommandSession<P>,
        input: &super::PromptInput,
        width: u16,
        height: u16,
    ) -> crate::Result<()> {
        let area = super::ratatui::layout::Rect::new(0, 0, width, height);
        self.wrapped = true;
        let areas = self.screen_for(files)?.areas(area, input);
        let width = areas.transcript.width;
        self.wrapped_transcript.reflow(width);
        self.wrapped_tools.reflow(width);
        self.reasoning_visible = viewport::reasoning(files)?;
        self.wrapped_reasoning.reflow(width);
        if !self.reasoning_visible {
            self.wrapped_reasoning.hide();
        }
        let side_width = areas.side.width.saturating_sub(2);
        self.side_wrapped = side_width > 0;
        if self.side_wrapped {
            self.wrapped_goal
                .update(&self.goal, self.goal_changed, side_width);
            self.wrapped_plans
                .update(&self.plans, self.plans_changed, side_width);
            self.goal_changed = self.goal.len();
            self.plans_changed = self.plans.len();
            self.wrapped_goal.view_from(
                super::RowPosition::default(),
                usize::from(areas.side.height),
            );
            self.wrapped_plans.view_from(
                super::RowPosition::default(),
                usize::from(areas.side.height),
            );
        }
        self.wrapped = true;
        let height = if width == 0 {
            0
        } else {
            usize::from(areas.transcript.height)
        };
        self.select_view(files, input, height)?;
        Ok(())
    }

    /// Yield before a bounded, cancellable full-search-count slice.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn search_progress(&mut self) {
        if !self.search_open || !self.search.pending() {
            std::future::pending::<()>().await;
        }
        tokio::task::yield_now().await;
        self.search.advance(
            super::TranscriptRows::new(&self.wrapped_transcript, &self.wrapped_tools),
            4096,
        );
    }

    fn compose_notices(&mut self) {
        self.composed_notices.clone_from(&self.notices);
        self.composed_notices.extend_from_slice(&self.file_notices);
        self.composed_notices
            .extend(self.confirmation_notice.map(Line::raw));
        if !self.busy {
            self.composed_notices
                .extend(self.outcome_notice.iter().cloned());
        }
    }

    /// Current followed lifecycle, used by the shared prompt's queue indicator.
    pub fn is_busy(&self) -> bool {
        self.busy
    }

    fn refresh_notices<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        files: &crate::client::CommandSession<P>,
        history_after: u64,
    ) -> crate::Result<()> {
        use crate::client::CommandFileRoot;
        let cut = (files.output_sequence(), history_after);
        if self.outcome_cut != Some(cut) {
            let output = files.output_page(0)?;
            self.outcome_notice = output
                .as_ref()
                .and_then(|value| {
                    value["error"].as_str().or_else(|| {
                        value["ok"]
                            .get("compacted")
                            .map(|_| "Compacted conversation")
                    })
                })
                .map(|message| Line::raw(message.escape_debug().to_string()));
            self.outcome_cut = Some(cut);
            self.compose_notices();
        }
        let feedback = files.state_file(CommandFileRoot::Session, "notifications.json")?;
        if self.notice_file.as_ref() != feedback {
            let messages: Vec<String> = feedback
                .map(|file| serde_json::from_slice(&file.bytes))
                .transpose()
                .map_err(|error| crate::Error::Invalid(error.to_string()))?
                .unwrap_or_default();
            self.file_notices = messages
                .into_iter()
                .map(|message| Line::raw(message.escape_debug().to_string()))
                .collect();
            self.notice_file = feedback.cloned();
            self.compose_notices();
        }
        Ok(())
    }

    /// Compose the panel selected by the followed command layout file.
    /// Unknown views retain the document overview until their widget is available.
    pub fn screen_for<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        files: &crate::client::CommandSession<P>,
    ) -> crate::Result<Screen<'_>> {
        use crate::client::CommandFileRoot;
        let layout = files.state_ref(CommandFileRoot::Session, "layout.json")?;
        let confirmation = layout
            .as_ref()
            .and_then(|layout| layout.get("confirmation"))
            .and_then(|confirmation| confirmation["request"]["kind"].as_str())
            .and_then(|kind| match kind {
                "clear_all" => Some("Clear all conversations? Enter yes or no."),
                "delete" => Some("Delete this conversation? Enter yes or no."),
                _ => None,
            });
        if self.confirmation_notice != confirmation {
            self.confirmation_notice = confirmation;
            self.compose_notices();
        }
        self.refresh_notices(files, files.history_after()?)?;
        let view = layout
            .as_ref()
            .and_then(|value| value.get("open_view"))
            .and_then(serde_json::Value::as_str);
        if view == Some("shortcuts") {
            self.help.refresh(files)?;
        }
        let settings = files.state_ref(CommandFileRoot::Config, "settings.json")?;
        let reasoning = settings
            .as_ref()
            .and_then(|value| value.get("reasoning"))
            .map(|value| {
                value
                    .as_bool()
                    .ok_or_else(|| crate::Error::Invalid("reasoning must be a boolean".into()))
            })
            .transpose()?
            .unwrap_or(false);
        let mut screen = self.screen();
        if view != Some("diff") {
            screen.diff = &[];
        }
        if reasoning {
            screen.reasoning = if self.wrapped {
                self.wrapped_reasoning.shown()
            } else {
                self.task.reasoning().1
            };
        }
        match view {
            Some("find")
                if layout
                    .as_ref()
                    .is_some_and(|value| value["view_input"] == "find") =>
            {
                let index = layout
                    .as_ref()
                    .and_then(|value| value["search_index"].as_i64())
                    .unwrap_or(0);
                let rows =
                    super::TranscriptRows::new(&self.wrapped_transcript, &self.wrapped_tools);
                screen.transcript_search = Some((rows, &self.search));
                screen.reasoning = &[];
                screen.status = Line::raw(format!(
                    "Find: {}/{}{} matches · Enter next · Shift+Enter previous · Escape closes",
                    self.search.position(index),
                    self.search.count(),
                    if self.search.pending() { "…" } else { "" }
                ));
            }
            Some("feedback") => {
                screen.feedback = layout
                    .as_ref()
                    .is_some_and(|value| value["view_input"] == "feedback");
            }
            Some("shortcuts") => {
                screen.overlay = &self.help.lines;
                screen.overlay_title = "Shortcuts";
            }
            Some("goal") => {
                screen.diff = &[];
                screen.plans = &[];
            }
            Some("plan") => {
                screen.diff = &[];
                screen.goal = &[];
            }
            _ => {}
        }
        Ok(screen)
    }

    fn source_rows(&self) -> &[Line<'static>] {
        self.transcript.get_or_init(|| {
            self.archive
                .rows()
                .iter()
                .chain(self.task.rows())
                .chain(&self.approval_rows)
                .cloned()
                .collect()
        })
    }

    /// Borrow cached rows for layout; no ClientViews or transcript clone per frame.
    pub fn screen(&self) -> Screen<'_> {
        // TODO(#548): compose split/auto diffs when that renderer is exposed.
        Screen {
            header: self.header.clone(),
            transcript: if self.wrapped {
                self.wrapped_transcript.shown()
            } else {
                self.source_rows()
            },
            tools: if self.wrapped {
                self.wrapped_tools.shown()
            } else {
                self.task.tools()
            },
            diff: &self.diff,
            goal: if self.wrapped && self.side_wrapped {
                self.wrapped_goal.shown()
            } else {
                &self.goal
            },
            plans: if self.wrapped && self.side_wrapped {
                self.wrapped_plans.shown()
            } else {
                &self.plans
            },
            hidden_tail: self.wrapped && self.top.is_some(),
            scrollbar: Some(if self.wrapped {
                self.source_window()
            } else {
                (0..0, 0)
            }),
            notifications: &self.composed_notices,
            status: self.status.clone(),
            ..Screen::default()
        }
    }
}

fn line(line: &acyclic_markdown::Line) -> Line<'static> {
    Line::from(
        line.spans
            .iter()
            .map(|span| {
                let style = style(span.style);
                // TODO(#604): emit queried OSC 8 link metadata through the terminal backend.
                Span::styled(super::display_text(&span.text).into_owned(), style)
            })
            .collect::<Vec<_>>(),
    )
}

fn style(source: acyclic_markdown::Style) -> Style {
    let mut style = Style::default();
    if let Some(acyclic_markdown::Color(r, g, b)) = source.foreground {
        style = style.fg(Color::Rgb(r, g, b));
    }
    for (set, modifier) in [
        (source.bold, Modifier::BOLD),
        (source.italic, Modifier::ITALIC),
        (source.dim, Modifier::DIM),
        (source.underline, Modifier::UNDERLINED),
        (source.strikethrough, Modifier::CROSSED_OUT),
    ] {
        if set {
            style = style.add_modifier(modifier);
        }
    }
    style
}

#[cfg(test)]
mod tail_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[path = "search_tests.rs"]
mod search_tests;
#[cfg(test)]
#[path = "thinking_tests.rs"]
mod thinking_tests;

#[cfg(test)]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[path = "frame_tests.rs"]
mod frame_tests;

#[cfg(test)]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[path = "thinking_frame_tests.rs"]
mod thinking_frame_tests;

#[cfg(test)]
#[cfg(all(
    feature = "filesystem-local",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
mod viewport_cost;

#[cfg(test)]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod scrollbar_tests;

#[cfg(all(test, feature = "filesystem-local", not(target_arch = "wasm32")))]
mod anchor_tests;

#[cfg(test)]
mod revision_tests;
