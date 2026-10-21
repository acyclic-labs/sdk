use super::{
    Screen,
    ratatui::{
        style::{Color, Modifier, Style},
        text::{Line, Span},
    },
};
use crate::{client::ClientViews, wire};
use acyclic_markdown::Markdown;

mod archive;
mod selection;
mod stream_text;
mod task;
mod tool;
use stream_text::StreamText;

/// Disposable incremental render cache. Authoritative state is always ClientViews
/// and the followed files; this cache is never serialized or written back.
#[derive(Default)]
pub struct ClientRenderer {
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
    reasoning_rows: Vec<Line<'static>>,
    transcript: Vec<Line<'static>>,
    wrapped: bool,
    transcript_changed: usize,
    reasoning_changed: usize,
    wrapped_transcript: super::WrappedLines,
    wrapped_tools: super::WrappedLines,
    tools_changed: usize,
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
    composed_notices: Vec<Line<'static>>,
    diff: Vec<Line<'static>>,
}

impl ClientRenderer {
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
        let key = authority.id.clone();
        if self.key != key {
            *self = Self {
                key,
                tool_details: self.tool_details,
                ..Self::default()
            };
        }
        #[cfg(test)]
        {
            self.folded_views += 1;
        }
        self.wrapped = false;
        self.side_wrapped = false;
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
        if self.task.tools_changed() {
            self.tools_changed = 0;
        }
        let prefix = self.archive.rows().len();
        let changed = if archive_changed < old_prefix || prefix != old_prefix {
            self.transcript.truncate(archive_changed);
            self.transcript
                .extend_from_slice(&self.archive.rows()[archive_changed..]);
            self.transcript.extend_from_slice(self.task.rows());
            archive_changed
        } else {
            let changed = prefix + task_changed;
            self.transcript.truncate(changed);
            self.transcript
                .extend_from_slice(&self.task.rows()[task_changed..]);
            changed
        };
        self.transcript_changed = self.transcript_changed.min(changed);
        let (changed, rows) = self.task.reasoning();
        self.reasoning_rows.truncate(changed);
        self.reasoning_rows.extend_from_slice(&rows[changed..]);
        self.reasoning_changed = self.reasoning_changed.min(changed);
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
        let settings = files.state(crate::client::CommandFileRoot::Config, "settings.json")?;
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
        let after = files
            .state(crate::client::CommandFileRoot::Session, "layout.json")?
            .and_then(|layout| layout["history_after"].as_u64())
            .unwrap_or(0);
        if self.tool_details != details || self.archive.after() != after {
            self.tool_details = details;
            self.task = task::TaskRows::default();
            self.transcript.clear();
            self.transcript_changed = 0;
            self.archive = archive::Archive::new(after);
            self.update(views, authority);
        }
        self.busy = files.pending_execution()? || self.task.busy();
        if let Some(marker) = self.header.spans.first_mut() {
            marker.content = std::borrow::Cow::Borrowed(if self.busy { "● " } else { "○ " });
        }
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
        self.wrapped = false;
        let areas = self.screen_for(files)?.areas(area, input);
        let width = areas.transcript.width;
        self.wrapped_transcript
            .update(&self.transcript, self.transcript_changed, width);
        self.wrap_tools(width);
        self.wrapped_reasoning
            .update(&self.reasoning_rows, self.reasoning_changed, width);
        self.transcript_changed = self.transcript.len();
        self.reasoning_changed = self.reasoning_rows.len();
        let side_width = areas.side.width.saturating_sub(2);
        self.side_wrapped = side_width > 0;
        if self.side_wrapped {
            self.wrapped_goal
                .update(&self.goal, self.goal_changed, side_width);
            self.wrapped_plans
                .update(&self.plans, self.plans_changed, side_width);
            self.goal_changed = self.goal.len();
            self.plans_changed = self.plans.len();
        }
        self.wrapped = true;
        let layout = files
            .state(crate::client::CommandFileRoot::Session, "layout.json")?
            .unwrap_or_default();
        self.search_open = layout["open_view"] == "find" && layout["view_input"] == "find";
        let rows = super::TranscriptRows::new(&self.wrapped_transcript, &self.wrapped_tools);
        self.search.project(rows, self.search_open);
        if self.search_open {
            self.search.update(
                rows,
                layout["search_query"].as_str().unwrap_or(""),
                layout["search_index"].as_i64().unwrap_or(0),
                input.theme.style(super::config::ThemeRole::Selection),
            );
            let height = usize::from(areas.transcript.height);
            let first = if self.search.count() != 0 {
                self.search
                    .selected_row(rows, layout["search_index"].as_i64().unwrap_or(0))
            } else {
                rows.len()
                    .saturating_sub(height)
                    .saturating_sub(layout["history_after"].as_u64().unwrap_or(0) as usize)
            }
            .min(rows.len().saturating_sub(height));
            self.search.prepare(rows, first, height);
        }
        Ok(())
    }

    fn wrap_tools(&mut self, width: u16) {
        self.wrapped_tools
            .update(self.task.tools(), self.tools_changed, width);
        self.tools_changed = self.task.tools().len();
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
            .extend(self.outcome_notice.iter().cloned());
    }

    /// Current followed lifecycle, used by the shared prompt's queue indicator.
    pub fn is_busy(&self) -> bool {
        self.busy
    }

    /// Compose the panel selected by the followed command layout file.
    /// Unknown views retain the document overview until their widget is available.
    pub fn screen_for<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        files: &crate::client::CommandSession<P>,
    ) -> crate::Result<Screen<'_>> {
        use crate::client::CommandFileRoot;
        let layout = files.state(CommandFileRoot::Session, "layout.json")?;
        let cut = (
            files.output_sequence(),
            layout
                .as_ref()
                .and_then(|value| value["history_after"].as_u64())
                .unwrap_or(0),
        );
        if self.outcome_cut != Some(cut) {
            let output = files.output_page(0)?;
            self.outcome_notice = output
                .as_ref()
                .and_then(|value| value["error"].as_str())
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
        let view = layout
            .as_ref()
            .and_then(|value| value.get("open_view"))
            .and_then(serde_json::Value::as_str);
        if view == Some("shortcuts") {
            self.help.refresh(files)?;
        }
        let settings = files.state(CommandFileRoot::Config, "settings.json")?;
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
        if reasoning {
            screen.reasoning = if self.wrapped {
                self.wrapped_reasoning.rows()
            } else {
                &self.reasoning_rows
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
                screen.search_row =
                    (self.search.count() != 0).then(|| self.search.selected_row(rows, index));
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

    /// Borrow cached rows for layout; no ClientViews or transcript clone per frame.
    pub fn screen(&self) -> Screen<'_> {
        // TODO(#495): bind the captured-volume snapshot/follow to the client diff fold.
        // TODO(#548): compose split/auto diffs when that renderer is exposed.
        Screen {
            header: self.header.clone(),
            transcript: if self.wrapped {
                self.wrapped_transcript.rows()
            } else {
                &self.transcript
            },
            tools: if self.wrapped {
                self.wrapped_tools.rows()
            } else {
                self.task.tools()
            },
            diff: &self.diff,
            goal: if self.wrapped && self.side_wrapped {
                self.wrapped_goal.rows()
            } else {
                &self.goal
            },
            plans: if self.wrapped && self.side_wrapped {
                self.wrapped_plans.rows()
            } else {
                &self.plans
            },
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
                let mut style = Style::default();
                if let Some(acyclic_markdown::Color(r, g, b)) = span.style.foreground {
                    style = style.fg(Color::Rgb(r, g, b));
                }
                for (set, modifier) in [
                    (span.style.bold, Modifier::BOLD),
                    (span.style.italic, Modifier::ITALIC),
                    (span.style.dim, Modifier::DIM),
                    (span.style.underline, Modifier::UNDERLINED),
                    (span.style.strikethrough, Modifier::CROSSED_OUT),
                ] {
                    if set {
                        style = style.add_modifier(modifier);
                    }
                }
                // TODO(#604): emit queried OSC 8 link metadata through the terminal backend.
                Span::styled(super::display_text(&span.text).into_owned(), style)
            })
            .collect::<Vec<_>>(),
    )
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
