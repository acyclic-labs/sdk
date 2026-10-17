use super::{
    Screen,
    ratatui::{
        style::{Color, Modifier, Style},
        text::{Line, Span},
    },
};
use crate::{
    client::TaskFileViews,
    model::{ModelDataPart, ToolResultContent},
    wire,
};
use acyclic_markdown::Markdown;

#[derive(Default)]
struct StreamText {
    markdown: Markdown,
    fed: usize,
    // Pending suffix repair: replace the last three displayed bytes, then append.
    provisional: bool,
    provisional_newline: bool,
    provisional_row: Option<usize>,
    #[cfg(test)]
    work: usize,
}

impl StreamText {
    fn push(&mut self, text: &str, pending: usize) -> usize {
        // Display the repairable suffix separately from the stable parser cut.
        // Replacing these at most three bytes never replays stable Markdown.
        let provisional = pending != 0;
        let text = &text[..text.len().saturating_sub(pending)];
        if text.len() < self.fed {
            *self = Self::default();
        }
        let previous_row = self.provisional_row.take();
        self.provisional = provisional;
        self.provisional_newline = text.ends_with('\n');
        let delta = text.get(self.fed..).unwrap_or_default();
        self.fed = text.len();
        #[cfg(test)]
        {
            self.work += delta.len();
        }
        let changed = self.markdown.push(delta);
        // The provisional glyph may be on an earlier row than the completed
        // Markdown leaf (for example an archive followed by whitespace).
        let changed = previous_row.map_or(changed, |row| changed.min(row));
        if provisional {
            changed.min(self.markdown.lines().len().saturating_sub(1))
        } else {
            changed
        }
    }
    fn rows(&mut self, changed: usize) -> Vec<Line<'static>> {
        let mut rows: Vec<_> = self
            .markdown
            .lines()
            .iter()
            .skip(changed)
            .map(line)
            .collect();
        if self.provisional {
            if rows.is_empty() || self.provisional_newline {
                rows.push(Line::default());
            }
            self.provisional_row = Some(changed + rows.len() - 1);
            let last = rows.last_mut().expect("provisional row");
            let style = last
                .spans
                .last()
                .map_or(Style::default(), |span| span.style);
            last.spans.push(Span::styled("\u{fffd}", style));
        }
        rows
    }
}

/// Disposable incremental render cache. Authoritative state is always TaskFileViews
/// and the followed files; this cache is never serialized or written back.
#[derive(Default)]
pub struct ClientRenderer {
    key: String,
    help: super::help::Help,
    busy: bool,
    tool_details: bool,
    seen: usize,
    stable: usize,
    turn: Option<crate::OperationId>,
    file_start: Option<crate::task_volume::FileKey>,
    reasoning_start: Option<crate::task_volume::FileKey>,
    stream: StreamText,
    reasoning: StreamText,
    reasoning_rows: Vec<Line<'static>>,
    transcript: Vec<Line<'static>>,
    wrapped: bool,
    transcript_changed: usize,
    reasoning_changed: usize,
    wrapped_transcript: super::WrappedLines,
    wrapped_reasoning: super::WrappedLines,
    wrapped_goal: super::WrappedLines,
    wrapped_plans: super::WrappedLines,
    side_wrapped: bool,
    goal_changed: usize,
    plans_changed: usize,
    goal_source: String,
    goal: Vec<Line<'static>>,
    plan_source: String,
    plans: Vec<Line<'static>>,
    header: Line<'static>,
    status: Line<'static>,
    notices: Vec<Line<'static>>,
    notice_file: Option<crate::task_volume::FileValue>,
    file_notices: Vec<Line<'static>>,
    outcome_cut: Option<(u64, u64)>,
    outcome_notice: Option<Line<'static>>,
    composed_notices: Vec<Line<'static>>,
    diff_cut: Option<(
        crate::conversation::VolumeRef,
        crate::resources::GenerationRef,
        crate::resources::GenerationRef,
    )>,
    diff: Vec<Line<'static>>,
}

impl ClientRenderer {
    #[cfg(test)]
    pub(crate) fn text_work(&self) -> usize {
        self.stream.work + self.reasoning.work
    }
    /// Update only the active agent. Stable transcript rows survive text deltas;
    /// the Markdown parser supplies the first changed open-block row.
    pub fn update(&mut self, views: &TaskFileViews, authority: &wire::Authority) {
        let key = crate::client::authority_key(authority);
        if self.key != key {
            *self = Self {
                key: key.clone(),
                tool_details: self.tool_details,
                ..Self::default()
            };
        }
        self.wrapped = false;
        if let Some(transcript) = views.transcripts.get(&key) {
            if transcript.messages.len() < self.seen {
                self.seen = 0;
                self.stable = 0;
            }
            if transcript.messages.len() != self.seen {
                self.transcript_changed = self.transcript_changed.min(self.stable);
                self.transcript.truncate(self.stable);
                for message in &transcript.messages[self.seen..] {
                    self.transcript.push(Line::styled(
                        format!("{:?}", message.kind),
                        Style::default().add_modifier(Modifier::BOLD),
                    ));
                    if let Some(text) = transcript.text.get(&message.id) {
                        self.transcript.extend(markdown(text));
                    } else if let Some(call) = message
                        .tool_call_id
                        .as_ref()
                        .and_then(|id| views.tools.get(&key)?.get(id))
                    {
                        self.transcript.extend(tool_rows(
                            &call.name,
                            &call.output,
                            self.tool_details,
                        ));
                    }
                }
                self.seen = transcript.messages.len();
                self.stable = self.transcript.len();
                self.stream = StreamText::default();
            }
            if self.turn != transcript.streaming_turn
                || self.file_start != transcript.file_start
                || self.reasoning_start != transcript.reasoning_start
            {
                self.turn = transcript.streaming_turn;
                self.file_start.clone_from(&transcript.file_start);
                self.reasoning_start.clone_from(&transcript.reasoning_start);
                self.reasoning = StreamText::default();
                self.stream = StreamText::default();
                self.transcript_changed = self.transcript_changed.min(self.stable);
                self.transcript.truncate(self.stable);
            }
            let changed_reasoning = self
                .reasoning
                .push(&transcript.reasoning, transcript.pending(true));
            self.reasoning_changed = self.reasoning_changed.min(changed_reasoning);
            self.reasoning_rows.truncate(changed_reasoning);
            self.reasoning_rows
                .extend(self.reasoning.rows(changed_reasoning));
            let changed = self
                .stream
                .push(&transcript.streaming_text, transcript.pending(false));
            self.transcript_changed = self.transcript_changed.min(self.stable + changed);
            self.transcript.truncate(self.stable + changed);
            self.transcript.extend(self.stream.rows(changed));
        } else {
            self.seen = 0;
            self.stable = 0;
            self.turn = None;
            self.stream = StreamText::default();
            self.transcript.clear();
            self.transcript_changed = 0;
            self.reasoning = StreamText::default();
            self.reasoning_rows.clear();
            self.reasoning_changed = 0;
        }
        if let Some(tools) = views.tools.get(&key) {
            for tool in tools.values().filter(|tool| tool.result.is_none()) {
                self.transcript
                    .extend(tool_rows(&tool.name, &tool.output, self.tool_details));
            }
        }
        if let Some(documents) = views.documents.get(&key) {
            let goal = documents.goal.as_deref().unwrap_or_default();
            if self.goal_source != goal {
                self.goal = markdown(goal);
                self.goal_changed = 0;
                self.goal_source = goal.into();
            }
            let plans = documents
                .plans
                .iter()
                .map(|(name, text)| format!("# {}\n{text}", name.as_str()))
                .collect::<Vec<_>>()
                .join("\n\n");
            if self.plan_source != plans {
                self.plans = markdown(&plans);
                self.plans_changed = 0;
                self.plan_source = plans;
            }
        } else {
            self.goal_source.clear();
            self.goal.clear();
            self.goal_changed = 0;
            self.plan_source.clear();
            self.plans.clear();
            self.plans_changed = 0;
        }
        let diff = views.diffs.get(&key);
        let cut = diff.map(|view| (view.volume(), view.base(), view.generation()));
        if self
            .diff_cut
            .as_ref()
            .map(|(volume, base, generation)| (volume, base, generation))
            != cut
        {
            self.diff = diff.map_or_else(Vec::new, |view| super::diff_lines(view.files()));
            self.diff_cut = cut.map(|(volume, base, generation)| {
                (volume.clone(), base.clone(), generation.clone())
            });
        }
        let task = views.tasks.get(&key);
        self.busy = task.is_some_and(|task| task.status == crate::core::LifecycleState::Active);
        self.header = Line::from(vec![
            Span::raw(if self.busy { "● " } else { "○ " }),
            Span::styled("GraphCoder", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(
                if task.is_some_and(|task| task.status == crate::core::LifecycleState::Cancelled) {
                    " ■ STOPPED"
                } else {
                    ""
                },
            ),
        ]);
        let usage = views
            .usage
            .get(&key)
            .map_or(String::new(), ToString::to_string);
        self.status = Line::raw(super::display_text(&usage).into_owned());
        self.notices = task
            .and_then(|task| task.reason.as_deref())
            .map(|text| vec![Line::raw(text.escape_debug().to_string())])
            .unwrap_or_default();
        self.compose_notices();
    }

    /// Apply followed tool visibility, rebuilding the disposable transcript only
    /// when this preference changes. The file remains the authoritative setting.
    pub fn configure<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        views: &TaskFileViews,
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
        if self.tool_details != details {
            self.tool_details = details;
            self.seen = 0;
            self.stable = 0;
            self.transcript.clear();
            self.transcript_changed = 0;
            self.stream = StreamText::default();
            self.update(views, authority);
        }
        self.busy = files.pending_execution()?
            || views
                .tasks
                .get(&self.key)
                .is_some_and(|task| task.status == crate::core::LifecycleState::Active);
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
        Ok(())
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

    /// Borrow cached rows for layout; no TaskFileViews or transcript clone per frame.
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

fn markdown(text: &str) -> Vec<Line<'static>> {
    let mut markdown = Markdown::default();
    markdown.push(text);
    markdown.lines().iter().map(line).collect()
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

fn tool_rows(name: &str, output: &ToolResultContent, details: bool) -> Vec<Line<'static>> {
    // TODO(#548): use specialized per-tool renderers when their API lands.
    let mut rows = vec![Line::styled(
        name.escape_debug().to_string(),
        Style::default().add_modifier(Modifier::BOLD),
    )];
    if !details {
        return rows;
    }
    let text = match output {
        ToolResultContent::Json { value } => value
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| value.to_string()),
        ToolResultContent::Parts { parts } => parts
            .iter()
            .map(|part| match part {
                ModelDataPart::Text { text } => text.as_str(),
                ModelDataPart::File { .. } => "[attachment]",
            })
            .collect::<Vec<_>>()
            .join("\n"),
    };
    if text.is_empty() {
        return rows;
    }
    let bounded = text.lines().take(20).collect::<Vec<_>>().join("\n");
    rows.extend(markdown(&bounded).into_iter().take(20));
    rows
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "thinking_tests.rs"]
mod thinking_tests;
