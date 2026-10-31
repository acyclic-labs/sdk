//! The interactive terminal: one conversation's settled turns, its pending
//! prompts, its streaming answer and its phase, over the shared prompt
//! editor and screen widgets.
use acyclic_harness::local::{
    Event,
    agent_conversation::{Conversation, PreparedHistoryAction},
};
use acyclic_harness::{
    agent::{AgentOutcome, ContextAction, Delivered, IntentId, Phase, RestoreMode, TurnOutcome},
    client::commands::{
        CommandCatalog, CommandQuery, CommandRequest, CommandResolution, DEFAULT_COMMANDS_JSON,
    },
    tui::{
        Action, AppAction, Editor, InputEffect, PromptInput, Screen, TranscriptRows, Viewport,
        key_event::InputEvent,
        ratatui::{
            layout::{Position, Rect, Size},
            style::{Modifier, Style},
            text::{Line, Span},
        },
        term::{Environment, FrameTerminal, ScreenMode, Tty},
    },
};
use acyclic_harness_client::{AgentControlFailure, AgentExchangeError, AgentReply};
use futures::{StreamExt as _, stream::LocalBoxStream};
use std::{sync::Arc, time::Instant};

type Failure = Box<dyn std::error::Error>;
mod diff;
mod query;

/// Settled turns shown when a conversation reopens.
const HISTORY: u64 = 64;
/// Rows of the newest tool call's output shown while the agent works.
const OUTPUT_ROWS: usize = 8;
/// What an interrupted turn shows in its place.
const CANCELLED: &str = "interaction cancelled";
const COMPACTED: &str = "Compacted conversation";
const REWOUND: &str = "Rewound to selected checkpoint";

/// How the session ended, as the process exit code: the last turn
/// succeeded, failed, or was cancelled.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Outcome {
    #[default]
    Settled,
    Failed,
    Cancelled,
}

impl Outcome {
    const fn code(self) -> u8 {
        match self {
            Self::Settled => 0,
            Self::Failed => 1,
            Self::Cancelled => 2,
        }
    }
}

/// A prompt the view opened itself, answered by the next submission.
enum Prompt {
    /// The feedback form: the submission is a report.
    Feedback,
    /// A command that runs once its yes arrives.
    Confirm(CommandRequest),
    /// Choose an action for the exact history turn selected on screen.
    History { at: u64 },
    /// An immutable history turn and the original restoration intent.
    Restore {
        at: u64,
        mode: RestoreMode,
        request: PreparedHistoryAction,
    },
    /// A fork keeps its complete original request across an uncertain result.
    Fork {
        at: u64,
        request: PreparedHistoryAction,
    },
}

#[derive(Default)]
struct View {
    transcript: TranscriptRows,
    /// Sent prompts not yet settled, by inbox sequence.
    pending: Vec<(u64, String)>,
    /// Sent compactions, rewinds and clears not yet settled, by inbox
    /// sequence.
    checkpoints: Vec<u64>,
    /// A cancel's settled status, shown once every entry it announced was:
    /// its inbox cursor and conversation tail.
    stopped: Option<(u64, u64)>,
    /// A cancelled entry already marked its turn's stop.
    cancel_marked: bool,
    /// The running turn's answer while it streams.
    reply: String,
    phase: Option<Phase>,
    /// The failed task's error while its conversation stays failed, shown
    /// whole on the status row.
    ended: Option<String>,
    /// A cancel was sent and its turn has not stopped yet.
    cancelling: bool,
    outcome: Outcome,
    /// A turn settled since the last frame: ring the bell.
    ring: bool,
    /// Rows scrolled back from the live tail.
    back: usize,
    width: u16,
    pending_dirty: bool,
    search: Option<(String, usize)>,
    /// The one prompt the view itself opened. The next submission answers
    /// it before anything else, a waiting approval included, and it hides
    /// that approval while open: one prompt is visible at a time, and every
    /// answer goes to it. A prompt opens only from a submission no prompt
    /// took, so a second never opens over the first.
    prompt: Option<Prompt>,
    /// A command's result or refusal, until the next prompt.
    notice: Option<String>,
    /// The pending approval's explanation, shown above the prompt.
    approval_prompt: Option<String>,
    /// Reports saved in this session.
    reports: Vec<String>,
}

impl View {
    /// A turn is open by the committed phase, whoever sent it: a reopened
    /// view's restored turn is as busy as one this view sent. This view's
    /// own unsettled sends cover only the moment before their status commits.
    fn busy(&self) -> bool {
        self.phase.as_ref().is_some_and(Phase::in_turn)
            || !self.pending.is_empty()
            || !self.checkpoints.is_empty()
    }

    fn approval(&self) -> Option<&IntentId> {
        match &self.phase {
            Some(Phase::WaitingApproval { id }) => Some(id),
            _ => None,
        }
    }

    /// Applies `event`; `seen` is the next conversation sequence the
    /// observer has not returned.
    async fn apply(
        &mut self,
        conversation: &Conversation,
        event: Event,
        seen: u64,
    ) -> Result<(), Failure> {
        match event {
            Event::Turn(entry) => {
                self.pending_dirty = true;
                self.checkpoints.retain(|inbox| *inbox != entry.inbox);
                if entry.outcome == TurnOutcome::RestoreConflict {
                    self.notice =
                        Some("The selected project heads changed; restoration was refused.".into());
                    return Ok(());
                }
                if entry.outcome == TurnOutcome::Completed
                    && let Some(action) = entry.input.context()
                {
                    match action {
                        ContextAction::Compact { .. } => self.transcript.push(false, COMPACTED),
                        ContextAction::Rewind => self.transcript.push(false, REWOUND),
                        ContextAction::Restore { mode, .. } => self.transcript.push(
                            false,
                            match mode {
                                RestoreMode::Files => "Restored selected project files",
                                RestoreMode::Context => "Restored selected context",
                                RestoreMode::Both => "Restored selected files and context",
                            },
                        ),
                        // A cleared conversation shows nothing before it.
                        ContextAction::Clear => self.transcript = TranscriptRows::default(),
                    }
                    self.outcome = Outcome::Settled;
                    self.ring = true;
                    return Ok(());
                }
                // A cancelled checkpoint selects nothing and shows nothing.
                if entry.input.context().is_some() {
                    return Ok(());
                }
                let cancelled = entry.outcome == TurnOutcome::Cancelled;
                let sent = self
                    .pending
                    .iter()
                    .position(|(pending, _)| *pending == entry.inbox)
                    .map(|at| self.pending.remove(at).1);
                // A turn's text is its committed entry's, a cancelled one's
                // too, whichever of it and its status arrived first.
                let (prompt, answer) = conversation.turn(&entry).await?;
                if let Some(text) = sent.or(prompt) {
                    self.transcript.push(true, &text);
                }
                if !answer.is_empty() {
                    self.transcript.push(false, &answer);
                }
                self.reply.clear();
                if cancelled {
                    self.transcript.push(false, CANCELLED);
                    self.outcome = Outcome::Cancelled;
                    // It marks the stop a cancel in flight would otherwise mark.
                    self.cancel_marked = self.cancelling || self.stopped.is_some();
                } else {
                    self.outcome = Outcome::Settled;
                }
                self.ring = true;
                self.stop(seen);
            }
            Event::Output => return Ok(()),
            Event::Status(status) => {
                self.ended = None;
                self.approval_prompt = match &status.phase {
                    Phase::WaitingApproval { id } => conversation.approval_prompt(id).await?,
                    _ => None,
                };
                // A settled agent consumed every request before its cursor:
                // a context request it passed without an entry was dropped
                // (cancelled) or failed, and is no longer work.
                if matches!(status.phase, Phase::Idle | Phase::Done { .. }) {
                    self.checkpoints.retain(|inbox| *inbox >= status.cursor);
                }
                if let Phase::Done {
                    outcome: AgentOutcome::Failed { message },
                } = &status.phase
                {
                    self.settle_pending();
                    self.ended = Some(format!("task {} failed: {message}", conversation.task()));
                    self.outcome = Outcome::Failed;
                    self.ring = true;
                } else if matches!(status.phase, Phase::Idle | Phase::Done { .. }) {
                    if std::mem::take(&mut self.cancelling) {
                        // The cancel's stop shows once the entries this status
                        // announced did: they hold the stopped turn's text.
                        self.stopped = Some((status.cursor, status.conversation));
                        // A cancelled approval fails its task; the next
                        // prompt clears the report.
                        if matches!(self.phase, Some(Phase::WaitingApproval { .. })) {
                            self.notice =
                                Some(format!("task {} failed: cancelled", conversation.task()));
                        }
                    } else if self.stopped.is_none() {
                        // A prompt the cursor passed shows with its turn's entry.
                        self.pending.retain(|(inbox, _)| *inbox >= status.cursor);
                    }
                }
                self.pending_dirty = true;
                self.phase = Some(status.phase);
                self.stop(seen);
            }
        }
        Ok(())
    }

    /// Runs a submitted or confirmed command; a compaction, rewind or clear
    /// shows when its checkpoint settles.
    async fn request(
        &mut self,
        conversation: &Conversation,
        request: CommandRequest,
    ) -> Result<(), Failure> {
        let id = acyclic_harness::OperationId::new().to_string();
        let inbox = match &request {
            CommandRequest::Compact { instructions } => {
                let instructions = instructions.trim();
                conversation
                    .compact(&id, (!instructions.is_empty()).then_some(instructions))
                    .await?
            }
            CommandRequest::Rewind => conversation.rewind(&id).await?,
            CommandRequest::ClearAll => conversation.clear_all(&id).await?,
            _ => {
                self.notice = Some(format!("{request:?} is not available in this view"));
                return Ok(());
            }
        };
        self.checkpoints.push(inbox);
        self.pending_dirty = true;
        Ok(())
    }

    /// Shows the pending prompts and the streamed answer as settled.
    fn settle_pending(&mut self) {
        for (_, prompt) in self.pending.drain(..) {
            self.transcript.push(true, &prompt);
        }
        if !self.reply.is_empty() {
            self.transcript
                .push(false, &std::mem::take(&mut self.reply));
        }
        self.pending_dirty = true;
    }

    /// Shows a cancel's stop once every entry its status announced was
    /// shown (`seen` is past them): the prompts it dropped without a turn,
    /// and its mark unless a cancelled entry already made it. The text a
    /// stopped turn saved is its entry's, never the streamed preview's.
    fn stop(&mut self, seen: u64) {
        let Some((cursor, announced)) = self.stopped else {
            return;
        };
        if seen < announced {
            return;
        }
        self.stopped = None;
        let mut dropped = Vec::new();
        self.pending.retain(|(inbox, prompt)| {
            let kept = *inbox >= cursor;
            if !kept {
                dropped.push(prompt.clone());
            }
            kept
        });
        for prompt in dropped {
            self.transcript.push(true, &prompt);
        }
        if !std::mem::take(&mut self.cancel_marked) {
            self.reply.clear();
            self.transcript.push(false, CANCELLED);
            self.outcome = Outcome::Cancelled;
            self.ring = true;
        }
        self.pending_dirty = true;
    }

    /// Append and transient-prompt changes leave settled rows wrapped once.
    fn rows(&mut self, width: u16) -> &[Line<'static>] {
        if self.pending_dirty || self.width != width {
            self.width = width;
            self.transcript.rows(width);
            let reply = (!self.reply.is_empty()).then_some((false, self.reply.as_str()));
            self.transcript.pending(
                self.pending
                    .iter()
                    .map(|(_, text)| (true, text.as_str()))
                    .chain(reply),
            );
            self.pending_dirty = false;
        }
        self.transcript.rows(width)
    }

    /// The commands and keys, from the same catalog completion uses.
    fn help(catalog: &CommandCatalog, input: &PromptInput) -> Vec<Line<'static>> {
        let mut lines = vec![Line::raw("Commands")];
        for command in catalog.commands() {
            let aliases = command
                .aliases
                .iter()
                .map(|alias| format!(" /{alias}"))
                .collect::<String>();
            lines.push(Line::raw(format!(
                "/{}{aliases} · {}",
                command.name, command.description
            )));
        }
        lines.push(Line::raw("Keys"));
        lines.extend(
            input
                .keymap
                .help()
                .lines()
                .map(|line| Line::raw(line.replace('\t', " · "))),
        );
        lines
    }

    /// The pending approval's explanation, one row per line.
    fn approval_rows(&self) -> Vec<Line<'static>> {
        self.approval_prompt
            .iter()
            .flat_map(|prompt| prompt.lines())
            .map(|line| Line::raw(line.to_owned()))
            .chain(std::iter::once(Line::raw(
                "y approves · n declines · /feedback reports · Esc cancels",
            )))
            .collect()
    }

    fn header(&self) -> Line<'static> {
        let mut spans = vec![
            Span::raw(if self.busy() { "● " } else { "○ " }),
            Span::styled("GraphCoder", Style::default().add_modifier(Modifier::BOLD)),
        ];
        if self.outcome == Outcome::Cancelled && !self.busy() {
            spans.push(Span::raw(" ■ STOPPED"));
        }
        Line::from(spans)
    }

    fn status(&self) -> String {
        if let Some(Prompt::Confirm(CommandRequest::ClearAll)) = &self.prompt {
            return "Clear all conversations? Enter yes or no.".into();
        }
        if let Some(notice) = self.notice.as_ref().or(self.ended.as_ref()) {
            return notice.clone();
        }
        if let Some(Prompt::Restore { at, mode, .. }) = &self.prompt {
            let target = match mode {
                RestoreMode::Files => "files",
                RestoreMode::Context => "context",
                RestoreMode::Both => "files and context",
            };
            return format!(
                "Restore {target} through cut {}? Enter yes or no. Child agents and external effects continue.",
                at + 1
            );
        }
        if let Some(Prompt::History { at }) = &self.prompt {
            return format!(
                "Cut {} selected. Enter files, context, both, or fork followed by the new task brief.",
                at + 1
            );
        }
        if let Some(Prompt::Fork { at, .. }) = &self.prompt {
            return format!(
                "Fork files and context through cut {}? Enter yes or no.",
                at + 1
            );
        }
        match (&self.phase, self.approval()) {
            (_, Some(id)) => format!(
                "approval {} requested: submit y to approve, n to decline",
                id.as_str()
            ),
            (Some(Phase::Done { outcome }), _) => format!("conversation ended: {outcome:?}"),
            _ if self.busy() => "working · Esc or Ctrl+C interrupts".into(),
            _ if self.transcript.truncated() => {
                "earlier transcript omitted · Ctrl+F finds prompt text".into()
            }
            _ => "Enter sends · Ctrl+F finds prompt text · Ctrl+D quits".into(),
        }
    }
}

/// The page keys scroll the transcript, or an open panel, by one page of
/// `page` rows.
fn paged(effect: InputEffect, page: usize) -> InputEffect {
    let page = isize::try_from(page).unwrap_or(isize::MAX);
    match effect {
        InputEffect::Host(Action::TranscriptUp) => InputEffect::Scroll(page),
        InputEffect::Host(Action::TranscriptDown) => InputEffect::Scroll(-page),
        other => other,
    }
}

/// The terminal's input batches, read on a thread of their own: the runtime's
/// single blocking worker stays free for the agent's file work, which would
/// otherwise wait behind a read that lasts until the next key. A full channel
/// holds the reader back; a dropped view ends it at its next batch.
fn terminal_events(
    mut tty: Tty,
) -> std::io::Result<LocalBoxStream<'static, std::io::Result<Vec<InputEvent>>>> {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    std::thread::Builder::new()
        .name("graphcoder-input".into())
        .spawn(move || {
            loop {
                let deadline = Instant::now() + std::time::Duration::from_secs(86_400);
                let events = tty.read_events(deadline);
                let failed = events.is_err();
                if matches!(&events, Ok(events) if events.is_empty()) {
                    continue;
                }
                if sender.blocking_send(events).is_err() || failed {
                    return;
                }
            }
        })?;
    Ok(
        futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|events| (events, receiver))
        })
        .boxed_local(),
    )
}

/// Runs the terminal view; `resume` reopens the last conversation's recent
/// turns, otherwise the view starts empty.
pub async fn run(
    conversation: Arc<Conversation>,
    inline: bool,
    resume: bool,
) -> Result<u8, Failure> {
    let mut tty = Tty::open()?;
    tty.negotiate(&Environment::capture())?;
    tty.setup(if inline {
        ScreenMode::Inline
    } else {
        ScreenMode::Alternate
    })?;
    let size = tty.size_reader();
    let (tty, output) = tty.split();
    let mut terminal = FrameTerminal::new(output, size()?)?;
    let mut keys = terminal_events(tty)?;
    let settled = conversation.settled().await?;
    let from = if resume {
        settled.saturating_sub(HISTORY)
    } else {
        settled
    };
    // Its settled turns, its phase and, while it works, the newest tool
    // call's output or its streaming answer.
    let mut events = conversation.observe(from).await?;
    let mut view = View::default();
    let mut output_rows = Vec::new();
    let catalog = CommandCatalog::builtins()?;
    let mut input = PromptInput::default();
    // The open panel's title and rows; Esc closes it.
    let mut overlay: Option<(&'static str, Vec<Line<'static>>)> = None;
    let mut query_open = false;
    let mut query_source = query::Source::Diff(None);
    let mut query_panel: Option<query::Panel> = None;
    let query_loading = [Line::raw("Loading…")];
    let mut query_future = std::pin::pin!(None);
    let mut query_refresh = false;
    let mut query_credit = 0;
    // The message id and text of the prompt the host last refused.
    let mut unsent: Option<(String, String)> = None;
    input
        .completion
        .set_file("commands/commands.json", Some(DEFAULT_COMMANDS_JSON))?;
    let mut viewport = Viewport::default();
    // An open panel's first shown row; panels open at their top.
    let mut overlay_top = 0usize;
    let start = Instant::now();
    loop {
        let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let size: Size = size()?;
        let busy = view.busy();
        input.busy = busy;
        let back = view.back;
        if std::mem::take(&mut view.ring) {
            terminal.bell();
        }
        let approval_rows = if overlay.is_none()
            && !query_open
            && view.prompt.is_none()
            && view.approval().is_some()
        {
            view.approval_rows()
        } else {
            Vec::new()
        };
        let feedback = matches!(view.prompt, Some(Prompt::Feedback));
        if let Some(panel) = query_panel.as_mut() {
            let area = Rect::from((Position::ORIGIN, size));
            panel
                .page
                .wrap(Screen::default().areas(area, &input).overlay_columns());
        }
        let (areas, page) = {
            let status = Line::raw(view.status());
            let screen = Screen {
                header: view.header(),
                transcript: view.rows(size.width.saturating_sub(1)),
                terminal: if busy { &output_rows[..] } else { &[] },
                status,
                overlay: overlay.as_ref().map_or_else(
                    || {
                        query_panel.as_ref().map_or_else(
                            || {
                                if query_open {
                                    &query_loading[..]
                                } else {
                                    &approval_rows[..]
                                }
                            },
                            |panel| &panel.page.lines[..],
                        )
                    },
                    |(_, lines)| &lines[..],
                ),
                overlay_title: overlay.as_ref().map_or(
                    if query_open {
                        query_source.title()
                    } else if approval_rows.is_empty() {
                        ""
                    } else {
                        "Approval"
                    },
                    |(title, _)| title,
                ),
                feedback,
                ..Screen::default()
            };
            let area = Rect::from((Position::ORIGIN, size));
            let tail = screen.tail_scroll(area, &input);
            let scroll = if overlay.is_some() || query_open {
                overlay_top = overlay_top.min(tail);
                overlay_top
            } else {
                tail.saturating_sub(back)
            };
            let mut areas = screen.areas(area, &input);
            terminal.draw(size, |frame| {
                areas = screen.render(frame, &input, &mut viewport, scroll, now);
            })?;
            (areas, usize::from(areas.transcript.height).max(1))
        };
        // Only inner content cells can advance the query. Reserve the hint;
        // a collapsed panel waits for the existing resize/input event.
        let credit = u32::from(areas.overlay_rows(1)).min(128);
        if query_open && query_credit != credit {
            query_credit = credit;
            query_panel = None;
            query_refresh = false;
            query_future.set(
                (credit != 0)
                    .then(|| query::load(Arc::clone(&conversation), credit, query_source.clone())),
            );
        }
        tokio::select! {
            result = async {
                match query_future.as_mut().as_pin_mut() {
                    Some(query) => query.await,
                    None => std::future::pending().await,
                }
            } => {
                query_future.set(None);
                match result {
                    Ok(panel) => {
                        query_panel = Some(panel);
                        overlay_top = 0;
                        if std::mem::take(&mut query_refresh) && credit != 0 {
                            query_future.set(Some(query::load(Arc::clone(&conversation), credit, query_source.clone())));
                        }
                    }
                    Err(error) => {
                        query_open = false;
                        query_panel = None;
                        view.notice = Some(error.to_string());
                    }
                }
            }
            event = events.next() => {
                let event = event?;
                let settled = matches!(&event, Event::Turn(_));
                if matches!(event, Event::Output) {
                    events.absorb().await?;
                }
                // Only a turn this view waited on rings; a reopened
                // history is not news.
                let waited = view.busy();
                view.apply(&conversation, event, events.seen()).await?;
                view.ring &= waited;
                if query_open && settled && credit != 0 && query_source.follows_tail() {
                    query_source = query_source.restart();
                    if query_future.is_none() {
                        query_future.set(Some(query::load(Arc::clone(&conversation), credit, query_source.clone())));
                    } else {
                        query_refresh = true;
                    }
                }
                // A settled turn clears the output and the streamed answer.
                if view.reply != events.reply() {
                    view.reply.clear();
                    view.reply.push_str(events.reply());
                    view.pending_dirty = true;
                }
                output_rows = events.output().lines().rev().take(OUTPUT_ROWS)
                    .map(|line| Line::raw(line.to_owned()))
                    .collect();
                output_rows.reverse();
            }
            batch = keys.next() => {
                for event in batch.ok_or("terminal input ended")?? {
                    let effects = areas
                        .event(&mut input, event, &mut viewport, now, &mut |_| None)
                        .collect::<Vec<_>>();
                    for effect in effects.into_iter().map(|effect| paged(effect, page)) {
                        match effect {
                            InputEffect::Submit { text, .. } => {
                                view.notice = None;
                                // The view's own prompt is the visible one:
                                // it takes this answer, never the approval.
                                match view.prompt.take() {
                                    Some(Prompt::Feedback) => {
                                        view.reports.push(text);
                                        view.notice = Some(format!(
                                            "Feedback saved locally ({} this session)",
                                            view.reports.len()
                                        ));
                                        continue;
                                    }
                                    Some(Prompt::Confirm(request)) => {
                                        if matches!(text.trim(), "y" | "Y" | "yes") {
                                            view.request(&conversation, request).await?;
                                        }
                                        continue;
                                    }
                                    Some(Prompt::Restore { at, mode, mut request }) => {
                                        if matches!(text.trim(), "y" | "Y" | "yes") {
                                            match request.execute().await {
                                                Ok(AgentReply::Delivered { delivery: Delivered::Committed { inbox } }) => {
                                                    view.checkpoints.push(inbox);
                                                    view.pending_dirty = true;
                                                }
                                                Ok(AgentReply::Delivered { delivery: Delivered::Closed }) => {
                                                    view.notice = Some("Restore refused: conversation is archived.".into());
                                                }
                                                Ok(_) => return Err("the endpoint answered another history action".into()),
                                                Err(error) => {
                                                    if matches!(error, AgentExchangeError::Control(AgentControlFailure::Refused { .. })) {
                                                        view.notice = Some(format!("Restore refused: {error}"));
                                                    } else {
                                                        view.notice = Some(format!("Restore not confirmed ({}): {error}. Enter yes to resend the same request.", error.code()));
                                                        view.prompt = Some(Prompt::Restore { at, mode, request });
                                                    }
                                                }
                                            }
                                        }
                                        continue;
                                    }
                                    Some(Prompt::History { at }) => {
                                        let mode = match text.trim() {
                                            "files" => Some(RestoreMode::Files),
                                            "context" => Some(RestoreMode::Context),
                                            "both" => Some(RestoreMode::Both),
                                            _ => None,
                                        };
                                        if let Some(mode) = mode {
                                            let prepared = async {
                                                let expected = if mode.files() { conversation.project_heads().await? } else { Vec::new() };
                                                conversation.prepare_restore(&acyclic_harness::OperationId::new().to_string(), at, mode, expected)
                                            }.await;
                                            match prepared {
                                                Ok(request) => view.prompt = Some(Prompt::Restore { at, mode, request }),
                                                Err(error) => {
                                                    view.prompt = Some(Prompt::History { at });
                                                    view.notice = Some(format!("Prepare restore failed before sending: {error}. Enter the restore mode to retry."));
                                                }
                                            }
                                        } else if let Some(brief) = text.trim().strip_prefix("fork ").map(str::trim).filter(|brief| !brief.is_empty()) {
                                            match conversation.prepare_fork(&acyclic_harness::OperationId::new().to_string(), at, brief.to_owned()) {
                                                Ok(request) => view.prompt = Some(Prompt::Fork { at, request }),
                                                Err(error) => {
                                                    view.prompt = Some(Prompt::History { at });
                                                    view.notice = Some(format!("Prepare fork failed before sending: {error}. Enter fork followed by the task brief to retry."));
                                                }
                                            }
                                        } else {
                                            view.prompt = Some(Prompt::History { at });
                                            view.notice = Some("Enter files, context, both, or fork followed by the new task brief.".into());
                                        }
                                        continue;
                                    }
                                    Some(Prompt::Fork { at, mut request }) => {
                                        if matches!(text.trim(), "y" | "Y" | "yes") {
                                            match request.execute().await {
                                                Ok(AgentReply::Spawned { agent, delivery: Delivered::Committed { .. } }) => view.notice = Some(format!("Fork published: {}", agent.as_str())),
                                                Ok(AgentReply::Spawned { delivery: Delivered::Closed, .. }) => view.notice = Some("Fork refused: conversation is archived.".into()),
                                                Ok(_) => return Err("the endpoint answered another history action".into()),
                                                Err(error) => {
                                                    if matches!(error, AgentExchangeError::Control(AgentControlFailure::Refused { .. })) {
                                                        view.notice = Some(format!("Fork refused: {error}"));
                                                    } else {
                                                        view.notice = Some(format!("Fork not confirmed ({}): {error}. Enter yes to resend the same request.", error.code()));
                                                        view.prompt = Some(Prompt::Fork { at, request });
                                                    }
                                                }
                                            }
                                        }
                                        continue;
                                    }
                                    None => {}
                                }
                                if query_open && matches!(query_source, query::Source::History(_)) && !text.trim_start().starts_with('/') {
                                    let selected = text.trim().parse::<u64>().ok()
                                        .and_then(|cut| query_panel.as_ref().and_then(|panel| panel.history_turn(cut)));
                                    if let Some(at) = selected {
                                        view.prompt = Some(Prompt::History { at });
                                        query_open = false;
                                        query_panel = None;
                                        query_future.set(None);
                                    } else {
                                        view.notice = Some("Enter an immutable cut displayed on this history page.".into());
                                    }
                                    continue;
                                }
                                // Commands stay usable while an approval
                                // waits; only other input answers it.
                                if let Some(id) = view.approval().cloned()
                                    && !text.trim_start().starts_with('/')
                                {
                                    match text.trim().to_ascii_lowercase().as_str() {
                                        "y" | "yes" => conversation.decide(&id, true).await?,
                                        "n" | "no" => conversation.decide(&id, false).await?,
                                        _ => {
                                            view.notice = Some("approval requires yes or no".into());
                                            input.editor = Editor::new(&text);
                                        }
                                    }
                                    continue;
                                }
                                match catalog.resolve(&text, u64::MAX) {
                                    Ok(None) => {}
                                    Ok(Some(CommandResolution::Submit { request: CommandRequest::Quit })) => {
                                        return Ok(view.outcome.code());
                                    }
                                    Ok(Some(CommandResolution::Select { query: CommandQuery::Shortcuts })) => {
                                        query_open = false;
                                        query_panel = None;
                                        query_future.set(None);
                                        overlay = Some(("Help", View::help(&catalog, &input)));
                                        overlay_top = 0;
                                        continue;
                                    }
                                    Ok(Some(CommandResolution::Select { query: CommandQuery::Diff })) => {
                                        overlay = None;
                                        query_open = true;
                                        query_source = query::Source::Diff(None);
                                        query_panel = None;
                                        query_refresh = false;
                                        query_credit = credit;
                                        query_future.set((credit != 0).then(|| query::load(Arc::clone(&conversation), credit, query_source.clone())));
                                        overlay_top = 0;
                                        continue;
                                    }
                                    Ok(Some(CommandResolution::Select { query: CommandQuery::Feedback })) => {
                                        view.prompt = Some(Prompt::Feedback);
                                        continue;
                                    }
                                    Ok(Some(CommandResolution::Confirm { request })) => {
                                        view.prompt = Some(Prompt::Confirm(request));
                                        continue;
                                    }
                                    Ok(Some(CommandResolution::Submit { request: CommandRequest::Rewind })) => {
                                        overlay = None;
                                        query_open = true;
                                        query_source = query::Source::History(None);
                                        query_panel = None;
                                        query_refresh = false;
                                        query_credit = credit;
                                        query_future.set((credit != 0).then(|| query::load(Arc::clone(&conversation), credit, query_source.clone())));
                                        overlay_top = 0;
                                        continue;
                                    }
                                    Ok(Some(CommandResolution::Submit { request })) => {
                                        view.request(&conversation, request).await?;
                                        continue;
                                    }
                                    Ok(Some(_)) => {
                                        view.notice = Some(format!("{} is not available in this view", text.trim()));
                                                                                continue;
                                    }
                                    Err(error) => {
                                        view.notice = Some(error.to_string());
                                        input.editor = Editor::new(&text);
                                        continue;
                                    }
                                }
                                if view.pending.len() >= HISTORY as usize
                                    || text.len().saturating_add(view.pending.iter().map(|(_, text)| text.len()).sum::<usize>()) > 1024 * 1024
                                {
                                    view.notice = Some("pending prompt display is full; wait for a turn to settle".into());
                                    input.editor = Editor::new(&text);
                                    continue;
                                }
                                // A refused prompt, such as an unknown `/name`, stays
                                // in the editor with its reason. Sending the same text
                                // again reuses its message id, so a send whose commit
                                // was uncertain replays exactly and never commits twice.
                                let id = match unsent.take() {
                                    Some((id, sent)) if sent == text => id,
                                    _ => acyclic_harness::OperationId::new().to_string(),
                                };
                                let inbox = match conversation.send(&id, &text).await {
                                    Ok(inbox) => inbox,
                                    Err(error) => {
                                        view.notice = Some(error.to_string());
                                        input.editor = Editor::new(&text);
                                        unsent = Some((id, text));
                                        continue;
                                    }
                                };
                                view.pending.push((inbox, text));
                                view.back = 0;
                                view.pending_dirty = true;
                            }
                            InputEffect::Host(Action::Escape) if overlay.is_some() => {
                                overlay = None;
                            }
                            InputEffect::Host(Action::Escape) if query_open => {
                                query_open = false;
                                query_panel = None;
                                query_future.set(None);
                            }
                            InputEffect::Host(Action::Escape) if view.prompt.is_some() => {
                                view.prompt = None;
                            }
                            InputEffect::Interrupt | InputEffect::Host(Action::Escape)
                                if view.busy() =>
                            {
                                view.cancelling = true;
                                conversation.cancel().await?;
                            }
                            InputEffect::Interrupt | InputEffect::Host(Action::Escape) => {}
                            InputEffect::Quit => return Ok(view.outcome.code()),
                            InputEffect::Scroll(rows) if query_open && query_future.is_none() && credit != 0 => {
                                if let Some(panel) = query_panel.as_ref()
                                    && !panel.page.page_boundary(&mut overlay_top, rows, areas.overlay_rows(0))
                                {
                                    continue;
                                }
                                let source = if rows < 0 {
                                    query_panel.as_mut().and_then(|panel| panel.page.next.take())
                                } else if query_panel.as_ref().is_some_and(|panel| panel.page.continued) {
                                    Some(query_source.restart())
                                } else {
                                    None
                                };
                                if let Some(source) = source {
                                    query_source = source;
                                    query_refresh = false;
                                    query_future.set(Some(query::load(Arc::clone(&conversation), credit, query_source.clone())));
                                }
                            }
                            InputEffect::Scroll(_) if query_open => {}
                            InputEffect::Scroll(rows) if overlay.is_some() => {
                                overlay_top = overlay_top.saturating_add_signed(-rows);
                            }
                            InputEffect::Scroll(rows) => {
                                view.back = view
                                    .back
                                    .saturating_add_signed(rows)
                                    .min(view.transcript.len().saturating_sub(page));
                            }
                            InputEffect::Host(Action::App(AppAction::Find)) => {
                                let query = input.editor.text();
                                let before = view.search.as_ref()
                                    .filter(|(previous, _)| previous == query)
                                    .map_or(view.transcript.len(), |(_, row)| *row);
                                if let Some(row) = view.transcript.find(query, before)
                                    .or_else(|| view.transcript.find(query, view.transcript.len()))
                                {
                                    view.back = view.transcript.len().saturating_sub(row + page);
                                    view.search = Some((query.to_owned(), row));
                                }
                            }
                            _ => {}
                        }
                    }
                }
                input.refresh_completions();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reopened_view_is_busy_while_its_restored_turn_waits() -> Result<(), Failure> {
        // A reopened view sent nothing: only the committed phase says a
        // turn is open, so Esc and Ctrl+C reach it.
        let mut view = View::default();
        view.phase = Some(Phase::WaitingChildren { any: false });
        assert!(view.busy());
        assert_eq!(view.status(), "working · Esc or Ctrl+C interrupts");
        for phase in [
            Phase::Running,
            Phase::WaitingApproval {
                id: IntentId::try_from("approval")?,
            },
            Phase::WaitingHost {
                host: "host".into(),
            },
            Phase::WaitingAlarm {
                deadline_unix_ms: 1,
            },
        ] {
            view.phase = Some(phase);
            assert!(view.busy());
        }
        // Negative controls: an idle or ended agent holds no turn.
        for phase in [
            Phase::Idle,
            Phase::Done {
                outcome: AgentOutcome::Succeeded { result: None },
            },
        ] {
            view.phase = Some(phase);
            assert!(!view.busy());
        }
        Ok(())
    }
}
