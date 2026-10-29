//! The interactive terminal: one conversation's settled turns, its pending
//! prompts, its streaming answer and its phase, over the shared prompt
//! editor and screen widgets.
use acyclic_harness::local::{
    Event,
    agent_conversation::{Conversation, checkpoint},
};
use acyclic_harness::{
    agent::{AgentOutcome, ContextAction, IntentId, Phase, TurnOutcome},
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
use futures::{StreamExt as _, stream::LocalBoxStream};
use std::{sync::Arc, time::Instant};

type Failure = Box<dyn std::error::Error>;

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

#[derive(Default)]
struct View {
    transcript: TranscriptRows,
    /// Sent prompts not yet settled, by inbox sequence.
    pending: Vec<(u64, String)>,
    /// Sent compactions, rewinds and clears not yet settled, by inbox
    /// sequence.
    checkpoints: Vec<u64>,
    /// Inbox sequences below this were shown by a cancel's stop: their
    /// cancelled entries are not shown again.
    cancel_shown: u64,
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
    /// A confirmed command awaiting its yes or no.
    confirm: Option<CommandRequest>,
    /// A command's result or refusal, until the next prompt.
    notice: Option<String>,
    /// The pending approval's explanation, shown above the prompt.
    approval_prompt: Option<String>,
    /// The feedback form is open: the next submission is a report.
    feedback: bool,
    /// Reports saved in this session.
    reports: Vec<String>,
}

impl View {
    fn busy(&self) -> bool {
        !self.pending.is_empty()
            || !self.checkpoints.is_empty()
            || matches!(self.phase, Some(Phase::Running))
    }

    fn approval(&self) -> Option<&IntentId> {
        match &self.phase {
            Some(Phase::WaitingApproval { id }) => Some(id),
            _ => None,
        }
    }

    async fn apply(&mut self, conversation: &Conversation, event: Event) -> Result<(), Failure> {
        match event {
            Event::Turn(entry) => {
                self.pending_dirty = true;
                self.checkpoints.retain(|inbox| *inbox != entry.inbox);
                if let Some(action) = checkpoint(&entry) {
                    match action {
                        ContextAction::Compact { .. } => self.transcript.push(false, COMPACTED),
                        ContextAction::Rewind => self.transcript.push(false, REWOUND),
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
                if cancelled && entry.inbox < self.cancel_shown {
                    return Ok(());
                }
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
                    self.cancel_marked = true;
                } else {
                    self.outcome = Outcome::Settled;
                }
                self.ring = true;
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
                    // After a cancel, a prompt the cursor passed without a
                    // turn was dropped by it; otherwise its turn's entry
                    // shows it.
                    let mut dropped = Vec::new();
                    self.pending.retain(|(inbox, prompt)| {
                        let kept = *inbox >= status.cursor;
                        if !kept {
                            dropped.push(prompt.clone());
                        }
                        kept
                    });
                    if std::mem::take(&mut self.cancelling) {
                        for prompt in dropped {
                            self.transcript.push(true, &prompt);
                        }
                        self.cancel_shown = status.cursor;
                        if !std::mem::take(&mut self.cancel_marked) {
                            self.interrupted();
                        }
                        // A cancelled approval fails its task; the next
                        // prompt clears the report.
                        if matches!(self.phase, Some(Phase::WaitingApproval { .. })) {
                            self.notice =
                                Some(format!("task {} failed: cancelled", conversation.task()));
                        }
                    }
                }
                self.pending_dirty = true;
                self.phase = Some(status.phase);
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

    /// The running turn stopped: its streamed answer stays, marked.
    fn interrupted(&mut self) {
        if !self.reply.is_empty() {
            self.transcript
                .push(false, &std::mem::take(&mut self.reply));
        }
        self.transcript.push(false, CANCELLED);
        self.outcome = Outcome::Cancelled;
        self.ring = true;
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
        if let Some(CommandRequest::ClearAll) = &self.confirm {
            return "Clear all conversations? Enter yes or no.".into();
        }
        if let Some(notice) = self.notice.as_ref().or(self.ended.as_ref()) {
            return notice.clone();
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
        let approval_rows = if overlay.is_none() && !view.feedback && view.approval().is_some() {
            view.approval_rows()
        } else {
            Vec::new()
        };
        let feedback = view.feedback;
        let (areas, page) = {
            let status = Line::raw(view.status());
            let screen = Screen {
                header: view.header(),
                transcript: view.rows(size.width.saturating_sub(1)),
                terminal: if busy { &output_rows[..] } else { &[] },
                status,
                overlay: overlay
                    .as_ref()
                    .map_or(&approval_rows[..], |(_, lines)| &lines[..]),
                overlay_title: overlay.as_ref().map_or(
                    if approval_rows.is_empty() {
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
            let scroll = if overlay.is_some() {
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
        tokio::select! {
            event = events.next() => {
                let event = event?;
                if matches!(event, Event::Output) {
                    events.absorb().await?;
                }
                // Only a turn this view waited on rings; a reopened
                // history is not news.
                let waited = view.busy();
                view.apply(&conversation, event).await?;
                view.ring &= waited;
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
                                // A report is never an approval answer.
                                if std::mem::take(&mut view.feedback) {
                                    view.reports.push(text);
                                    view.notice = Some(format!(
                                        "Feedback saved locally ({} this session)",
                                        view.reports.len()
                                    ));
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
                                if let Some(request) = view.confirm.take() {
                                    if matches!(text.trim(), "y" | "Y" | "yes") {
                                        view.request(&conversation, request).await?;
                                    }
                                    continue;
                                }
                                match catalog.resolve(&text, u64::MAX) {
                                    Ok(None) => {}
                                    Ok(Some(CommandResolution::Submit { request: CommandRequest::Quit })) => {
                                        return Ok(view.outcome.code());
                                    }
                                    Ok(Some(CommandResolution::Select { query: CommandQuery::Shortcuts })) => {
                                        overlay = Some(("Help", View::help(&catalog, &input)));
                                        overlay_top = 0;
                                        continue;
                                    }
                                    Ok(Some(CommandResolution::Select { query: CommandQuery::Feedback })) => {
                                        view.feedback = true;
                                        continue;
                                    }
                                    Ok(Some(CommandResolution::Confirm { request })) => {
                                        view.confirm = Some(request);
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
                                let id = acyclic_harness::OperationId::new().to_string();
                                let inbox = conversation.send(&id, &text).await?;
                                view.pending.push((inbox, text));
                                view.back = 0;
                                view.pending_dirty = true;
                            }
                            InputEffect::Host(Action::Escape) if overlay.is_some() => {
                                overlay = None;
                            }
                            InputEffect::Host(Action::Escape) if view.feedback => {
                                view.feedback = false;
                            }
                            InputEffect::Interrupt | InputEffect::Host(Action::Escape)
                                if view.busy() || view.approval().is_some() =>
                            {
                                view.cancelling = true;
                                conversation.cancel().await?;
                            }
                            InputEffect::Interrupt | InputEffect::Host(Action::Escape) => {}
                            InputEffect::Quit => return Ok(view.outcome.code()),
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
