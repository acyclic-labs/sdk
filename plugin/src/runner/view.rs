//! The interactive terminal: one conversation's settled turns, its pending
//! prompts, its streaming answer and its phase, over the shared prompt
//! editor and screen widgets.
//!
//! The transcript is a window over the conversation stream: settled turns
//! append as they commit, older ones page in from the stream as the view
//! scrolls toward them, and nothing shown is evicted or capped. The
//! streaming answer is the observer's delta projection, rewrapped alone.
use acyclic_harness::local::{
    Event, LiveKind,
    agent_conversation::{Conversation, PreparedHistoryAction, UsageQuery, checkpoint},
};
use acyclic_harness::{
    agent::{
        AgentOutcome, ContextAction, ConversationEntry, Delivered, IntentId, Phase, RestoreMode,
        TurnOutcome,
    },
    client::commands::{CommandCatalog, CommandQuery, CommandRequest, CommandResolution},
    executor::ToolCard,
    interaction::{ApprovalBinding, ApprovalVerdict},
    tui::{
        Action, AppAction, BlockKind, Editor, InputEffect, PromptInput, QueryController, QueryPage,
        Screen, Thinking, TranscriptRows, Viewport,
        config::Token,
        context_summary,
        key_event::InputEvent,
        ratatui::{
            layout::{Position, Rect, Size},
            style::{Modifier, Style},
            text::{Line, Span},
        },
        term::{Environment, FrameTerminal, ScreenMode, Tty},
        usage_summary,
    },
};
use acyclic_harness_client::{AgentControlFailure, AgentExchangeError, AgentReply};
use futures::{StreamExt as _, stream::LocalBoxStream};
use std::{sync::Arc, time::Instant};

type Failure = Box<dyn std::error::Error>;
use acyclic_harness::completion::Exit;
mod diff;
mod query;
mod session;

async fn usage(mut query: UsageQuery) -> (UsageQuery, acyclic_harness::Result<bool>) {
    let result = query.advance().await;
    (query, result)
}

/// An older page of the transcript, for the view's `epoch`: the settled
/// turns at `from..to` as blocks, oldest first, and whether one of them
/// cleared the conversation, so nothing before the page shows.
struct Older {
    epoch: u64,
    from: u64,
    cleared: bool,
    blocks: Vec<(BlockKind, String)>,
}

async fn older(
    conversation: Arc<Conversation>,
    epoch: u64,
    from: u64,
    to: u64,
) -> Result<Older, String> {
    let read = async {
        let mut page = Older {
            epoch,
            from,
            cleared: false,
            blocks: Vec::new(),
        };
        for entry in conversation.entries(from, to).await? {
            if let Some(action) = checkpoint(&entry) {
                match notice(&action) {
                    Some(text) => page.blocks.push((BlockKind::Answer, text)),
                    // A cleared conversation shows nothing before it.
                    None => {
                        page.blocks.clear();
                        page.cleared = true;
                    }
                }
            } else {
                page.blocks.extend(turn(&conversation, &entry).await?);
            }
        }
        acyclic_harness::Result::Ok(page)
    };
    read.await.map_err(|error| error.to_string())
}

/// A settled turn's blocks: its prompt, its tool cards, its answer and,
/// if it was interrupted, the mark. A cancelled checkpoint shows nothing.
async fn turn(
    conversation: &Conversation,
    entry: &ConversationEntry,
) -> acyclic_harness::Result<Vec<(BlockKind, String)>> {
    if entry.input.context().is_some() {
        return Ok(Vec::new());
    }
    let turn = conversation.turn(entry).await?;
    let mut blocks = Vec::new();
    blocks.extend(turn.prompt.map(|prompt| (BlockKind::Prompt, prompt)));
    blocks.extend(work(turn.tools, turn.answer));
    if entry.outcome == TurnOutcome::Cancelled {
        blocks.push((BlockKind::Answer, CANCELLED.into()));
    }
    Ok(blocks)
}

/// A turn's work after its prompt: a collapsed card per tool call, then
/// its answer, if any.
fn work(tools: Vec<ToolCard>, answer: String) -> impl Iterator<Item = (BlockKind, String)> {
    tools
        .into_iter()
        .map(|card| {
            (
                BlockKind::Tool {
                    failed: card.failed,
                },
                card.line(),
            )
        })
        .chain((!answer.is_empty()).then_some((BlockKind::Answer, answer)))
}

/// What a settled checkpoint shows; a clear shows nothing, before it too.
fn notice(action: &ContextAction) -> Option<String> {
    match action {
        ContextAction::Compact { .. } => Some(COMPACTED.into()),
        ContextAction::Rewind => Some(REWOUND.into()),
        ContextAction::Restore { at, mode, .. } => Some(format!("Restored {mode:?} at turn {at}")),
        ContextAction::Clear => None,
    }
}

/// Rows of the newest tool call's output, or of the running reasoning,
/// shown while the agent works.
const OUTPUT_ROWS: usize = 8;
/// What an interrupted turn shows in its place.
const CANCELLED: &str = "interaction cancelled";
const COMPACTED: &str = "Compacted conversation";
const REWOUND: &str = "Rewound to selected checkpoint";

/// How the session ended, as the process exit: the last turn succeeded,
/// failed (status 1), or was cancelled (status 2).
#[derive(Default, PartialEq, Eq)]
enum Outcome {
    #[default]
    Settled,
    Failed(String),
    Cancelled,
}

impl View {
    /// The process exit, each nonzero status with its reason.
    fn exit(&self) -> Result<(), Failure> {
        match &self.outcome {
            Outcome::Settled => Ok(()),
            Outcome::Failed(message) => Err(Box::new(Exit::failed(message.clone()))),
            Outcome::Cancelled => Err(Box::new(Exit::new(
                std::num::NonZeroU8::MIN.saturating_add(1),
                "the last turn was cancelled",
            ))),
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
    /// A fork names its admitted role before preparing the immutable request.
    ForkRole { at: u64 },
    /// The selected role and history cut, awaiting the new task brief.
    ForkBrief { at: u64, role: String },
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
    /// Inputs below this observed frontier cannot become pending again when
    /// an uncertain command later replays its original delivery receipt.
    settled_before: u64,
    /// A cancel's settled status, shown once every entry it announced was:
    /// its inbox cursor and conversation tail.
    stopped: Option<(u64, u64)>,
    /// A cancelled entry already marked its turn's stop.
    cancel_marked: bool,
    /// The running turn's answer while it streams.
    reply: String,
    phase: Option<Phase>,
    /// A cancel was sent and its turn has not stopped yet.
    cancelling: bool,
    outcome: Outcome,
    /// A turn settled since the last frame: ring the bell.
    ring: bool,
    /// Rows scrolled back from the live tail.
    back: usize,
    width: u16,
    /// The pending prompts changed, or a settled turn cleared them.
    pending_dirty: bool,
    /// The streaming answer changed: only its rows rewrap.
    tail_dirty: bool,
    /// The first conversation sequence the transcript shows.
    top: u64,
    /// The first it may page in: 0 for a resumed view, the tail for a new
    /// one, and after a clear, the turn after it.
    floor: u64,
    /// Advanced by a clear, so a page read before it is not shown.
    epoch: u64,
    search: Option<(String, usize)>,
    /// The one prompt the view itself opened. The next submission answers
    /// it before anything else, a waiting approval included, and it hides
    /// that approval while open: one prompt is visible at a time, and every
    /// answer goes to it. A prompt opens only from a submission no prompt
    /// took, so a second never opens over the first.
    prompt: Option<Prompt>,
    /// A command's result or refusal, until the next prompt.
    notice: Option<String>,
    /// Original waiting-status identity of the action displayed by the pager.
    approval_status: Option<u64>,
    /// Exact immutable action whose final rows were painted.
    approval_shown: Option<acyclic_harness_client::PendingApproval>,
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

    fn checkpoint_sent(&mut self, inbox: u64) {
        if inbox >= self.settled_before && !self.checkpoints.contains(&inbox) {
            self.checkpoints.push(inbox);
            self.pending_dirty = true;
        }
    }

    fn observe_settled_before(&mut self, cursor: u64) {
        self.settled_before = self.settled_before.max(cursor);
        self.checkpoints
            .retain(|inbox| *inbox >= self.settled_before);
    }

    fn approval(&self) -> Option<&IntentId> {
        match &self.phase {
            Some(Phase::WaitingApproval { id, .. }) => Some(id),
            _ => None,
        }
    }

    /// Prepare an answer from the displayed status, before any dispatch can
    /// await. Commands stay available; an absent binding never falls back to
    /// querying a different pending action.
    fn approval_answer(
        &self,
        text: &str,
    ) -> Result<Option<(IntentId, ApprovalBinding, ApprovalVerdict)>, &'static str> {
        let Some(Phase::WaitingApproval { id, binding }) = &self.phase else {
            return Ok(None);
        };
        let answer = text.trim();
        if answer.starts_with('/') {
            return Ok(None);
        }
        let verdict = if answer.eq_ignore_ascii_case("y") || answer.eq_ignore_ascii_case("yes") {
            ApprovalVerdict::Approve
        } else if answer.eq_ignore_ascii_case("r") || answer.eq_ignore_ascii_case("rule") {
            ApprovalVerdict::ApproveRule
        } else if answer.eq_ignore_ascii_case("n") || answer.eq_ignore_ascii_case("no") {
            ApprovalVerdict::Deny
        } else {
            return Err("approval requires y (once), r (exact rule), or n (deny)");
        };
        let binding = binding.as_ref().ok_or("approval action is unavailable")?;
        if !self
            .approval_shown
            .as_ref()
            .is_some_and(|shown| &shown.id == id && &shown.binding == binding)
        {
            return Err("Review the complete approval action before deciding (PgDn: more)");
        }
        Ok(Some((id.clone(), binding.clone(), verdict)))
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
                self.observe_settled_before(entry.inbox.saturating_add(1));
                if let Some(action) = checkpoint(&entry) {
                    match notice(&action) {
                        Some(text) => self.transcript.push(BlockKind::Answer, &text),
                        // A cleared conversation shows nothing before it,
                        // and pages nothing in from before it.
                        None => {
                            self.transcript.clear();
                            self.top = seen;
                            self.floor = seen;
                            self.epoch += 1;
                        }
                    }
                    self.outcome = Outcome::Settled;
                    self.ring = true;
                    return Ok(());
                }
                // A failed context request settles with the same visible
                // failure as a prompt; it does not select a checkpoint.
                if entry.input.context().is_some() {
                    if let TurnOutcome::Failed { message } = &entry.outcome {
                        self.failed(conversation, message);
                    }
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
                let turn = conversation.turn(&entry).await?;
                if let Some(text) = sent.or(turn.prompt) {
                    self.transcript.push(BlockKind::Prompt, &text);
                }
                for (kind, text) in work(turn.tools, turn.answer) {
                    self.transcript.push(kind, &text);
                }
                self.reply.clear();
                if cancelled {
                    self.transcript.push(BlockKind::Answer, CANCELLED);
                    self.outcome = Outcome::Cancelled;
                    // It marks the stop a cancel in flight would otherwise mark.
                    self.cancel_marked = self.cancelling || self.stopped.is_some();
                } else if let TurnOutcome::Failed { message } = &entry.outcome {
                    // The turn failed; the conversation stays open.
                    self.failed(conversation, message);
                } else {
                    self.outcome = Outcome::Settled;
                }
                self.ring = true;
                self.stop(seen);
            }
            Event::Delta => return Ok(()),
            Event::Status(status) => {
                if !matches!((&status.phase, &self.approval_shown),
                    (Phase::WaitingApproval { id, binding: Some(binding) }, Some(shown))
                    if id == &shown.id && binding == &shown.binding)
                {
                    self.approval_shown = None;
                }
                self.approval_status = match &status.phase {
                    Phase::WaitingApproval { id, binding } => conversation
                        .pending_approval(id)
                        .await?
                        .filter(|pending| Some(&pending.binding) == binding.as_ref())
                        .map(|pending| pending.status),
                    _ => None,
                };
                // A settled agent consumed every request before its cursor:
                // a context request it passed without an entry was dropped
                // (cancelled) or failed, and is no longer work.
                if matches!(status.phase, Phase::Idle | Phase::Done { .. }) {
                    self.observe_settled_before(status.cursor);
                }
                if let Phase::Done {
                    outcome: AgentOutcome::Failed { message },
                } = &status.phase
                {
                    self.settle_pending();
                    self.failed(conversation, message);
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
            CommandRequest::Rewind {} => conversation.rewind(&id).await?,
            CommandRequest::ClearAll {} => conversation.clear_all(&id).await?,
            _ => {
                self.notice = Some(format!("{request:?} is not available in this view"));
                return Ok(());
            }
        };
        self.checkpoint_sent(inbox);
        Ok(())
    }

    /// Records a failure in the transcript, where it stays visible.
    fn failed(&mut self, conversation: &Conversation, message: &str) {
        self.transcript.push(
            BlockKind::Answer,
            &format!("task {} failed: {message}", conversation.task()),
        );
        self.outcome = Outcome::Failed(message.into());
        self.ring = true;
    }

    /// Shows the pending prompts and the streamed answer as settled.
    fn settle_pending(&mut self) {
        for (_, prompt) in self.pending.drain(..) {
            self.transcript.push(BlockKind::Prompt, &prompt);
        }
        if !self.reply.is_empty() {
            self.transcript
                .push(BlockKind::Answer, &std::mem::take(&mut self.reply));
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
            self.transcript.push(BlockKind::Prompt, &prompt);
        }
        if !std::mem::take(&mut self.cancel_marked) {
            self.reply.clear();
            self.transcript.push(BlockKind::Answer, CANCELLED);
            self.outcome = Outcome::Cancelled;
            self.ring = true;
        }
        self.pending_dirty = true;
    }

    /// Settled rows are wrapped once; a pending-prompt change rewraps the
    /// prompts and the tail, and a delta rewraps only the tail.
    fn rows(&mut self, width: u16) -> &[Line<'static>] {
        if self.pending_dirty || self.width != width {
            self.width = width;
            self.transcript.rows(width);
            self.transcript
                .pending(self.pending.iter().map(|(_, text)| text.as_str()));
            self.pending_dirty = false;
            self.tail_dirty = true;
        }
        if std::mem::take(&mut self.tail_dirty) {
            self.transcript.tail(Some(self.reply.as_str()));
        }
        self.transcript.rows(width)
    }

    /// The next older page to read, `from..self.top`, while fewer than two
    /// pages of rows are above the viewport. A page is `page` turns, each at
    /// least one row, so one read fills the shown rows whatever their size.
    fn older_page(&self, page: usize) -> Option<u64> {
        let above = self.transcript.len().saturating_sub(self.back + page);
        (self.top > self.floor && above < page.saturating_mul(2))
            .then(|| self.top.saturating_sub(page as u64).max(self.floor))
    }

    /// Keep the saved search hit on the same row of text as older rows arrive.
    fn prepend(&mut self, older: Older) {
        if older.epoch != self.epoch {
            return;
        }
        let added = self.transcript.prepend(older.blocks);
        if let Some((_, row)) = &mut self.search {
            *row += added;
        }
        self.top = older.from;
        if older.cleared {
            self.floor = older.from;
        }
    }

    /// Find the preceding hit, wrapping to the newest after the oldest.
    fn find(&mut self, query: &str, page: usize) {
        let before = self
            .search
            .as_ref()
            .filter(|(previous, _)| previous == query)
            .map_or(self.transcript.len(), |(_, row)| *row);
        if let Some(row) = self
            .transcript
            .find(query, before)
            .or_else(|| self.transcript.find(query, self.transcript.len()))
        {
            self.back = self.transcript.len().saturating_sub(row + page);
            self.search = Some((query.to_owned(), row));
        }
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
        if let Some(Prompt::Confirm(CommandRequest::ClearAll {})) = &self.prompt {
            return "Clear all conversations? Enter yes or no.".into();
        }
        if let Some(notice) = &self.notice {
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
                "Cut {} selected. Enter files, context, both, or fork.",
                at + 1
            );
        }
        if let Some(Prompt::ForkRole { at }) = &self.prompt {
            return format!("Fork cut {}: enter the child's role name.", at + 1);
        }
        if let Some(Prompt::ForkBrief { at, role }) = &self.prompt {
            return format!("Fork cut {} as {role}: enter the new task brief.", at + 1);
        }
        if let Some(Prompt::Fork { at, .. }) = &self.prompt {
            return format!(
                "Fork files and context through cut {}? Enter yes or no.",
                at + 1
            );
        }
        match (&self.phase, self.approval()) {
            (_, Some(id)) => format!("approval {} · y once · r exact rule · n deny", id.as_str()),
            (Some(Phase::Done { outcome }), _) => format!("conversation ended: {outcome:?}"),
            _ if self.busy() => "working · Esc or Ctrl+C interrupts".into(),
            _ => "Enter sends · Ctrl+F finds prompt text · Ctrl+D quits".into(),
        }
    }
}

/// The page keys scroll the transcript, or an open panel, by one page of
/// `page` rows.
/// The status line of a refused action. One marker distinguishes every
/// refusal from informational notices, so any reader of the screen (a person
/// or an acceptance driver) sees that the action failed.
fn refusal(reason: impl std::fmt::Display) -> Option<String> {
    Some(format!("\u{2717} {reason}"))
}

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
                let events = tty.read_events(None);
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
/// turns, paged in from the stream as they show, otherwise the view starts
/// empty.
pub async fn run(
    conversation: Arc<Conversation>,
    inline: bool,
    resume: bool,
    terminal: Option<Tty>,
    price_file: Option<String>,
) -> Result<(), Failure> {
    let mut ui = session::Session::open(&conversation).await?;
    if let Some(text) = price_file {
        ui.prices(&conversation, text).await?;
    }
    if !resume && ui.open_view().is_some() {
        ui.select(&conversation, None).await?;
    }
    let mut tty = match terminal {
        Some(terminal) => terminal,
        None => Tty::open()?,
    };
    let profile = tty.negotiate(&Environment::capture())?.clone();
    tty.setup(if inline {
        ScreenMode::Inline
    } else {
        ScreenMode::Alternate
    })?;
    let size = tty.size_reader();
    let (tty, output) = tty.split();
    let mut terminal = FrameTerminal::new(output, size()?)?;
    let mut keys = terminal_events(tty)?;
    // Its settled turns from the tail on, its phase and, while it works, the
    // newest tool call's output or its streaming answer or reasoning. A
    // resumed view replays its newest turn, whose outcome is the session's,
    // and pages the turns before it in as it shows them.
    let from = if resume {
        Some(conversation.settled().await?.saturating_sub(1))
    } else {
        None
    };
    let mut events = conversation.observe(from).await?;
    let mut view = View {
        top: events.seen(),
        floor: if resume { 0 } else { events.seen() },
        ..View::default()
    };
    let mut older_future = std::pin::pin!(None);
    let mut output_rows = Vec::new();
    let mut input = PromptInput::default();
    ui.config
        .apply(&mut input, &profile)
        .map_err(|errors| format!("UI configuration: {errors:?}"))?;
    view.transcript.set_theme(input.theme);
    view.pending_dirty = true;
    view.tail_dirty = true;
    // The open panel's title and rows; Esc closes it.
    let mut overlay: Option<(&'static str, Vec<Line<'static>>)> = None;
    let mut queries = QueryController::<query::Source, query::Context>::default();
    let query_loading = [Line::raw("Loading…")];
    let mut query_future = std::pin::pin!(None);
    if resume {
        view.notice = apply_open_view(
            ui.open_view(),
            &mut overlay,
            &mut queries,
            query_future.as_mut(),
            &ui.catalog,
            &input,
        );
    }
    let mut files_future = std::pin::pin!(session::follow(
        Arc::clone(&conversation),
        ui.snapshot.generation.clone()
    ));
    let mut usage_prices = Arc::clone(&ui.prices);
    let mut usage_future = std::pin::pin!(Some(usage(
        conversation.usage_query_with_prices(Arc::clone(&usage_prices))
    )));
    let mut usage_page = QueryPage::<()>::new(vec![Line::raw("Loading usage…")], None, false);
    let usage_unavailable = [Line::raw("Usage unavailable")];
    // Retain the failed query's exact frontier and contribution. Redraw cannot
    // restart replay or turn an unavailable source into a successful total.
    let mut usage_stopped: Option<(UsageQuery, acyclic_harness::Error)> = None;
    // The message id and text of the prompt the host last refused.
    let mut unsent: Option<(String, String)> = None;
    ui.complete(&mut input)?;
    let mut viewport = Viewport::default();
    // An open panel's first shown row; panels open at their top.
    let mut overlay_top = 0usize;
    // Replacement cancels a consumer, not the approval. Only a failed read
    // prevents reopening the same original action.
    let mut approval_failed = None;
    let mut approval_reviewed = None;
    let start = Instant::now();
    loop {
        // Any session read can first observe a price publication, including
        // a layout command racing the file follow. Replace the projection
        // once for that original file, never for a redraw or layout-only cut.
        if !Arc::ptr_eq(&usage_prices, &ui.prices) {
            usage_prices = Arc::clone(&ui.prices);
            usage_future.set(Some(usage(
                conversation.usage_query_with_prices(Arc::clone(&usage_prices)),
            )));
            usage_stopped = None;
            usage_page = QueryPage::new(vec![Line::raw("Loading usage…")], None, false);
            if matches!(queries.source(), Some(query::Source::Cost)) {
                queries.select(query::Source::Cost, query_future.as_mut());
                overlay_top = 0;
            }
        }
        let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let size: Size = size()?;
        usage_page.wrap(size.width);
        let busy = view.busy();
        input.busy = busy;
        let back = view.back;
        if std::mem::take(&mut view.ring) {
            terminal.bell();
        }
        if let Some(query::Source::Approval { status, .. }) = queries.source()
            && *status != view.approval_status
        {
            queries.close(query_future.as_mut());
        }
        if let Some(status) = view.approval_status {
            if approval_failed != Some(status)
                && overlay.is_none()
                && queries.source().is_none()
                && view.prompt.is_none()
            {
                queries.select(
                    query::Source::Approval {
                        request: view
                            .approval()
                            .ok_or("pending approval has no request")?
                            .clone(),
                        binding: match &view.phase {
                            Some(Phase::WaitingApproval {
                                binding: Some(binding),
                                ..
                            }) => binding.clone(),
                            _ => return Err("pending approval has no binding".into()),
                        },
                        status: Some(status),
                        offset: 0,
                    },
                    query_future.as_mut(),
                );
                overlay_top = 0;
            }
        } else {
            approval_failed = None;
        }
        let feedback = matches!(view.prompt, Some(Prompt::Feedback));
        if let Some(panel) = queries.page.as_mut() {
            let area = Rect::from((Position::ORIGIN, size));
            panel.wrap(Screen::default().areas(area, &input).overlay_columns());
        }
        let (areas, page) = {
            let status = Line::raw(view.status());
            let header = view.header();
            view.rows(size.width.saturating_sub(1));
            let (transcript, links, code) = view.transcript.presentation();
            let mut screen = Screen {
                header,
                transcript,
                links,
                code,
                terminal: if busy { &output_rows[..] } else { &[] },
                status,
                usage: if usage_stopped.is_some() {
                    &usage_unavailable[..]
                } else {
                    &usage_page.lines
                },
                overlay: overlay.as_ref().map_or_else(
                    || {
                        queries.page.as_ref().map_or_else(
                            || {
                                if queries.source().is_some() {
                                    &query_loading[..]
                                } else {
                                    &[]
                                }
                            },
                            |panel| &panel.lines[..],
                        )
                    },
                    |(_, lines)| &lines[..],
                ),
                overlay_title: overlay.as_ref().map_or(
                    if let Some(source) = queries.source() {
                        source.title()
                    } else {
                        ""
                    },
                    |(title, _)| title,
                ),
                feedback,
                ..Screen::default()
            };
            let area = Rect::from((Position::ORIGIN, size));
            let scroll = screen.tail_scroll(area, &input).saturating_sub(back);
            let mut areas = screen.areas(area, &input);
            let panel = overlay.is_some() || queries.source().is_some();
            if panel {
                overlay_top = overlay_top.min(areas.overlay_tail(screen.overlay.len()));
                screen.overlay_scroll = overlay_top;
            }
            terminal.draw_linked(size, None, |frame| {
                areas = screen.render(frame, &input, &mut viewport, scroll, now);
                screen.hyperlinks(&areas, scroll)
            })?;
            if overlay.is_none()
                && let Some(status) = query::reviewed(&queries, overlay_top, areas.overlay_rows(0))
            {
                approval_reviewed = Some(status);
                if let Some(Phase::WaitingApproval {
                    id,
                    binding: Some(binding),
                }) = &view.phase
                    && view.approval_status == Some(status)
                {
                    view.approval_shown = Some(acyclic_harness_client::PendingApproval {
                        id: id.clone(),
                        binding: binding.clone(),
                        status,
                    });
                }
            }
            // Page keys scroll the open panel by its page, else the transcript.
            let page = if panel {
                areas.overlay_rows(0)
            } else {
                areas.transcript.height
            };
            (areas, usize::from(page).max(1))
        };
        // Only inner content cells can advance the query. Reserve the hint;
        // a collapsed panel waits for the existing resize/input event.
        let credit = u32::from(areas.overlay_rows(1)).min(128);
        queries.start(credit, query_future.as_mut(), |source, rows| {
            query::load(
                Arc::clone(&conversation),
                rows,
                source.clone(),
                Arc::clone(&ui.prices),
            )
        });
        // Older turns page in as the view nears them: one read at a time,
        // started by the frame that showed the need, never retried.
        if older_future.is_none()
            && let Some(from) = view.older_page(page)
        {
            older_future.set(Some(older(
                Arc::clone(&conversation),
                view.epoch,
                from,
                view.top,
            )));
        }
        let mut previous_layout = None;
        let mut files_changed = false;
        tokio::select! {
            snapshot = &mut files_future => {
                let snapshot = snapshot?;
                previous_layout = ui.layout_digest();
                ui.snapshot = snapshot;
                ui.project()?;
                files_changed = true;
            }
            () = async {
                if areas.animation {
                    // A visible activity label owns its next frame. Idle and
                    // reduced-motion views keep waiting on input or updates.
                    let delay = Thinking::FRAME_MS - now % Thinking::FRAME_MS;
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                } else {
                    std::future::pending().await
                }
            } => {}
            (query, result) = async {
                match usage_future.as_mut().as_pin_mut() {
                    Some(query) => query.await,
                    None => std::future::pending().await,
                }
            } => {
                usage_future.set(None);
                match result {
                    Ok(changed) => {
                        if changed && query.synchronized() {
                            let mut cost = usage_summary(query.totals());
                            cost.spans.insert(0, Span::raw("This agent · "));
                            usage_page = QueryPage::new(vec![context_summary(query.model(), query.context(), query.open_children()), cost], None, false);
                        }
                        usage_future.set(Some(usage(query)));
                    }
                    Err(error) => {
                        view.notice = refusal(format_args!("Usage query ended: {error}"));
                        usage_stopped = Some((query, error));
                    }
                }
            }
            read = async {
                match older_future.as_mut().as_pin_mut() {
                    Some(read) => read.await,
                    None => std::future::pending().await,
                }
            } => {
                older_future.set(None);
                match read {
                    Ok(older) => view.prepend(older),
                    Err(error) => {
                        // The rows shown stay; nothing older is read again.
                        view.floor = view.top;
                        view.notice = refusal(format_args!("Older turns unavailable: {error}"));
                    }
                }
            }
            result = async {
                match query_future.as_mut().as_pin_mut() {
                    Some(query) => query.await,
                    None => std::future::pending().await,
                }
            } => {
                let approval = match queries.source() {
                    Some(query::Source::Approval { status, .. }) => *status,
                    _ => None,
                };
                match queries.finish(result, query_future.as_mut()) {
                    Ok(()) => {
                        overlay_top = 0;
                    },
                    Err(error) => {
                        if approval.is_some() {
                            approval_failed = approval;
                        }
                        view.notice = refusal(format_args!("Filesystem query ended: {error}"));
                    },
                }
            }
            event = events.next() => {
                let event = event?;
                let settled = matches!(&event, Event::Turn(_));
                if matches!(event, Event::Delta) {
                    events.absorb().await?;
                }
                // Only a turn this view waited on rings; a reopened
                // history is not news.
                let waited = view.busy();
                view.apply(&conversation, event, events.seen()).await?;
                if let Some(query::Source::Approval { request, binding, .. }) = queries.source()
                    && !matches!(&view.phase, Some(Phase::WaitingApproval { id, binding: Some(current) }) if id == request && current == binding)
                {
                    queries.close(query_future.as_mut());
                    overlay_top = 0;
                }
                view.ring &= waited;
                if settled {
                    queries.invalidate(query::Source::follows_tail);
                }
                // A settled turn clears the output and the streamed text.
                let (kind, live) = events.live();
                let answer = if kind == LiveKind::Answer { live } else { "" };
                if view.reply != answer {
                    view.reply.clear();
                    view.reply.push_str(answer);
                    view.tail_dirty = true;
                }
                // Reasoning shows muted in the strip tool output uses.
                let style = if kind == LiveKind::Reasoning {
                    input.theme.token(Token::Muted).add_modifier(Modifier::ITALIC)
                } else {
                    Style::default()
                };
                output_rows = if kind == LiveKind::Answer { "" } else { live }
                    .lines().rev().take(OUTPUT_ROWS)
                    .map(|line| Line::styled(line.to_owned(), style))
                    .collect();
                output_rows.reverse();
            }
            batch = keys.next() => {
                previous_layout = ui.layout_digest();
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
                                                    view.checkpoint_sent(inbox);
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
                                        } else if text.trim() == "fork" {
                                            view.prompt = Some(Prompt::ForkRole { at });
                                        } else {
                                            view.prompt = Some(Prompt::History { at });
                                            view.notice = Some("Enter files, context, both, or fork.".into());
                                        }
                                        continue;
                                    }
                                    Some(Prompt::ForkRole { at }) => {
                                        view.prompt = Some(if text.trim().is_empty() {
                                            Prompt::ForkRole { at }
                                        } else {
                                            Prompt::ForkBrief { at, role: text.trim().to_owned() }
                                        });
                                        continue;
                                    }
                                    Some(Prompt::ForkBrief { at, role }) => {
                                        if text.trim().is_empty() {
                                            view.prompt = Some(Prompt::ForkBrief { at, role });
                                        } else {
                                            match conversation.prepare_fork(&acyclic_harness::OperationId::new().to_string(), at, role.clone(), text) {
                                                Ok(request) => view.prompt = Some(Prompt::Fork { at, request }),
                                                Err(error) => {
                                                    view.prompt = Some(Prompt::ForkBrief { at, role });
                                                    view.notice = Some(format!("Prepare fork failed before sending: {error}"));
                                                }
                                            }
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
                                if matches!(queries.source(), Some(query::Source::History(_))) && !text.trim_start().starts_with('/') {
                                    let selected = text.trim().parse::<u64>().ok()
                                        .and_then(|cut| queries.page.as_ref().and_then(|panel| panel.context.history_turn(cut)));
                                    if let Some(at) = selected {
                                        view.prompt = Some(Prompt::History { at });
                                        queries.close(query_future.as_mut());
                                        ui.select(&conversation, None).await?;
                                        files_changed = true;
                                    } else {
                                        view.notice = Some("Enter an immutable cut displayed on this history page.".into());
                                    }
                                    continue;
                                }
                                // Commands stay usable while an approval
                                // waits; only other input answers it.
                                if view.approval().is_some() && !text.trim_start().starts_with('/')
                                    && (view.approval_status.is_none() || approval_reviewed != view.approval_status)
                                {
                                    view.notice = refusal("Review the complete approval action before deciding (PgDn: more)");
                                    input.editor = Editor::new(&text);
                                    continue;
                                }
                                match view.approval_answer(&text) {
                                    Ok(Some((id, binding, approved))) => {
                                        if let Err(error) = conversation.decide_with_binding(&id, binding, approved).await {
                                            view.notice = refusal(error);
                                            input.editor = Editor::new(&text);
                                        }
                                        continue;
                                    }
                                    Err(reason) => {
                                        view.notice = refusal(reason);
                                        input.editor = Editor::new(&text);
                                        continue;
                                    }
                                    Ok(None) => {}
                                }
                                match ui.catalog.resolve(&text, u64::MAX) {
                                    Ok(None) => {}
                                    Ok(Some(CommandResolution::Submit { request: CommandRequest::Quit {} })) => {
                                        return view.exit();
                                    }
                                    Ok(Some(resolution @ CommandResolution::Submit { request: CommandRequest::Theme { .. } })) => {
                                        match ui.submit(&conversation, &resolution).await {
                                            Ok(()) => files_changed = true,
                                            Err(error) => view.notice = Some(error.to_string()),
                                        }
                                        continue;
                                    }
                                    Ok(Some(CommandResolution::Select { query: selected @ (CommandQuery::Shortcuts | CommandQuery::Diff | CommandQuery::History | CommandQuery::Cost) })) => {
                                        match ui.select(&conversation, Some(&selected)).await {
                                            Ok(()) => {
                                                files_changed = true;
                                            }
                                            Err(error) => view.notice = Some(error.to_string()),
                                        }
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
                                    Ok(Some(CommandResolution::Submit { request })) => {
                                        view.request(&conversation, request).await?;
                                        continue;
                                    }
                                    Ok(Some(_)) => {
                                        view.notice = Some(format!("{} is not available in this view", text.trim()));
                                                                                continue;
                                    }
                                    Err(error) => {
                                        view.notice = refusal(error);
                                        input.editor = Editor::new(&text);
                                        continue;
                                    }
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
                                        view.notice = refusal(error);
                                        input.editor = Editor::new(&text);
                                        unsent = Some((id, text));
                                        continue;
                                    }
                                };
                                view.pending.push((inbox, text));
                                view.back = 0;
                                view.pending_dirty = true;
                            }
                            InputEffect::Host(Action::Escape) if view.prompt.is_some() => {
                                view.prompt = None;
                            }
                            InputEffect::Host(Action::Escape)
                                if matches!(queries.source(), Some(query::Source::Approval { .. })) =>
                            {
                                view.cancelling = true;
                                conversation.cancel().await?;
                            }
                            InputEffect::Host(Action::Escape) if overlay.is_some() || queries.source().is_some() => {
                                match ui.select(&conversation, None).await {
                                    Ok(()) => {
                                        files_changed = true;
                                    }
                                    Err(error) => view.notice = Some(error.to_string()),
                                }
                            }

                            InputEffect::Interrupt | InputEffect::Host(Action::Escape)
                                if view.busy() =>
                            {
                                view.cancelling = true;
                                conversation.cancel().await?;
                            }
                            InputEffect::Interrupt | InputEffect::Host(Action::Escape) => {}
                            InputEffect::Quit => return view.exit(),
                            InputEffect::Scroll(rows) if queries.source().is_some() && credit != 0 => {
                                queries.scroll(&mut overlay_top, rows, areas.overlay_rows(0),
                                    query_future.as_mut(), query::Source::newest);
                            }
                            InputEffect::Scroll(_) if queries.source().is_some() => {}
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
                                view.find(input.editor.text(), usize::from(areas.transcript.height));
                            }
                            _ => {}
                        }
                    }
                    // A batch can submit a panel and then Escape. Publish its
                    // visible state before the next event decides what to close.
                    if files_changed
                        && let Some(notice) = apply_session_layout(
                            &ui,
                            &mut previous_layout,
                            &mut overlay,
                            &mut queries,
                            query_future.as_mut(),
                            &input,
                        )
                    {
                        view.notice = notice.or(view.notice.take());
                        overlay_top = 0;
                    }
                }
                input.refresh_completions();
            }
        }
        if files_changed {
            ui.complete(&mut input)?;
            ui.config
                .apply(&mut input, &profile)
                .map_err(|errors| format!("UI configuration: {errors:?}"))?;
            view.transcript.set_theme(input.theme);
            view.pending_dirty = true;
            view.tail_dirty = true;
            if overlay.as_ref().is_some_and(|(title, _)| *title == "Help") {
                overlay = Some(("Help", View::help(&ui.catalog, &input)));
            }
            if let Some(notice) = apply_session_layout(
                &ui,
                &mut previous_layout,
                &mut overlay,
                &mut queries,
                query_future.as_mut(),
                &input,
            ) {
                view.notice = notice.or(view.notice.take());
                overlay_top = 0;
            }
            // A command can reopen past an in-flight follow. Apply the whole
            // committed projection before advancing either path's follow cut.
            files_future.set(session::follow(
                Arc::clone(&conversation),
                ui.snapshot.generation.clone(),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn restore_receipts_cannot_reopen_observed_settlement() -> Result<(), Failure> {
        use acyclic_harness::{
            LocalHarness,
            context::{CompactionPolicy, ModelContextCapacity},
            local::WorkspaceMounting,
            model::Model,
            test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
        };
        let directory = tempfile::tempdir()?;
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace)?;
        let provider = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Text("before".into()),
                ScriptedTurn::Text("after".into()),
            ])
            .with_context_capacity(ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: Some(4096),
            }),
        );
        let harness = Arc::new(
            LocalHarness::open(
                local_harness_config(
                    &workspace,
                    (
                        Model::new("scripted".into(), "test".into(), "1".into())?,
                        provider.clone(),
                    ),
                )?
                .without_user_settings()
                .state_directory(directory.path().join("state"))
                .workspace_mounting(WorkspaceMounting::Physical)
                .compaction(CompactionPolicy::Disabled),
            )
            .await?,
        );
        let conversation = Conversation::open(harness.connect().await?);
        let mut events = conversation.observe(None).await?;
        for id in ["before", "after"] {
            let inbox = conversation.send(id, id).await?;
            conversation.settle(id, inbox, &mut events).await?;
        }
        drop(events);
        let (mut events, phase) = conversation.observe_current().await?;
        let mut view = View {
            phase: Some(phase),
            ..View::default()
        };
        for (id, lose_reply) in [("lost-restore", true), ("observed-restore", false)] {
            let mut request =
                conversation.prepare_restore(id, 0, RestoreMode::Context, Vec::new())?;
            let inbox = match request.execute().await.map_err(|error| error.to_string())? {
                AgentReply::Delivered {
                    delivery: Delivered::Committed { inbox },
                } => inbox,
                _ => return Err("restore did not retain its original delivery".into()),
            };
            if !lose_reply {
                view.checkpoint_sent(inbox);
                assert!(view.busy(), "an unobserved receipt must remain pending");
            }
            let mut turn = false;
            while !turn {
                let event = events.next().await?;
                turn |= matches!(&event, Event::Turn(entry) if entry.inbox == inbox);
                view.apply(&conversation, event, events.seen()).await?;
            }
            assert!(!view.busy());
            let replay = request.execute().await.map_err(|error| error.to_string())?;
            assert!(
                matches!(replay, AgentReply::Delivered { delivery: Delivered::Committed { inbox: repeated } } if repeated == inbox)
            );
            view.checkpoint_sent(inbox);
            assert!(
                !view.busy(),
                "an original settled receipt became pending again"
            );
        }
        assert_eq!(
            provider.request_count(),
            2,
            "restoration must not redispatch the model"
        );
        Ok(())
    }

    #[test]
    fn search_visits_each_older_match_across_prepended_pages() {
        let mut view = View::default();
        view.rows(16);
        view.transcript.push(BlockKind::Prompt, "match newest\nend");
        view.find("match", 1);
        for (from, text, expected) in [
            (
                2,
                "match oldest\npadding wraps over several rows\nmatch middle\npadding",
                "match middle",
            ),
            (0, "match earliest\npadding", "match oldest"),
        ] {
            view.prepend(Older {
                epoch: view.epoch,
                from,
                cleared: false,
                blocks: vec![(BlockKind::Prompt, text.into())],
            });
            view.find("match", 1);
            let row = view.search.as_ref().unwrap().1;
            assert!(view.transcript.rows(16)[row].to_string().contains(expected));
            assert_eq!(view.back, view.transcript.len() - row - 1);
        }
        for expected in ["match earliest", "match newest"] {
            view.find("match", 1);
            let row = view.search.as_ref().unwrap().1;
            assert!(view.transcript.rows(16)[row].to_string().contains(expected));
            assert_eq!(view.back, view.transcript.len() - row - 1);
        }
    }

    #[test]
    fn saved_views_this_terminal_cannot_render_leave_it_open() -> Result<(), Failure> {
        use acyclic_harness::command_request::{CommandFileSnapshot, layout_view, view_changes};
        let catalog = CommandCatalog::builtins()?;
        let input = PromptInput::default();
        let mut overlay = None;
        let mut queries = QueryController::<query::Source, query::Context>::default();
        let mut pending = std::pin::pin!(None::<std::future::Ready<()>>);
        // Views the shared writer saves and this terminal does not render.
        for query in [
            CommandQuery::Model,
            CommandQuery::Plan,
            CommandQuery::Feedback,
        ] {
            let changes = view_changes(
                &CommandFileSnapshot::new(),
                Some(&query),
                acyclic_harness::OperationId::new(),
            )?;
            let saved = layout_view(&changes[0].contents)?.ok_or("view was not saved")?;
            queries.select(query::Source::Diff(None), pending.as_mut());
            let notice = apply_open_view(
                Some(saved),
                &mut overlay,
                &mut queries,
                pending.as_mut(),
                &catalog,
                &input,
            );
            assert!(notice.is_some_and(|notice| notice.contains(saved)));
            assert!(overlay.is_none() && queries.source().is_none());
        }
        // Negative control: a renderable view opens without a notice.
        let notice = apply_open_view(
            Some("shortcuts"),
            &mut overlay,
            &mut queries,
            pending.as_mut(),
            &catalog,
            &input,
        );
        assert!(notice.is_none() && overlay.is_some());
        Ok(())
    }

    #[test]
    fn approval_answers_retain_the_observed_action_across_status_changes() -> Result<(), Failure> {
        let mut view = View::default();
        assert!(view.approval_answer("yes")?.is_none());
        let id = IntentId::try_from("approval")?;
        let binding = ApprovalBinding {
            operation_id: acyclic_harness::OperationId::from_bytes([3; 16]),
            action_digest: [4; 32],
        };
        view.phase = Some(Phase::WaitingApproval {
            id: id.clone(),
            binding: Some(binding.clone()),
        });
        assert!(view.approval_answer("yes").is_err());
        view.approval_shown = Some(acyclic_harness_client::PendingApproval {
            id: id.clone(),
            binding: binding.clone(),
            status: 1,
        });
        for (text, approved) in [("y", true), (" YES ", true), ("n", false), (" No ", false)] {
            assert_eq!(
                view.approval_answer(text)?,
                Some((id.clone(), binding.clone(), approved.into()))
            );
        }
        assert_eq!(
            view.approval_answer("r")?,
            Some((id.clone(), binding.clone(), ApprovalVerdict::ApproveRule))
        );
        assert!(view.approval_answer(" /feedback")?.is_none());
        assert!(view.approval_answer("approve something else").is_err());
        let prepared = view.approval_answer("yes")?.unwrap();
        let mut replacement = binding.clone();
        replacement.action_digest[0] ^= 1;
        view.phase = Some(Phase::WaitingApproval {
            id: id.clone(),
            binding: Some(replacement.clone()),
        });
        assert_eq!(prepared, (id.clone(), binding.clone(), true.into()));
        assert_ne!(prepared.1, replacement);
        assert!(view.approval_answer("yes").is_err());
        view.approval_shown = Some(acyclic_harness_client::PendingApproval {
            id: id.clone(),
            binding: replacement.clone(),
            status: 2,
        });
        assert_eq!(
            view.approval_answer("yes")?,
            Some((id.clone(), replacement, true.into()))
        );
        view.phase = Some(Phase::WaitingApproval { id, binding: None });
        assert!(view.approval_answer("yes").is_err());
        assert!(view.approval_answer("no").is_err());
        assert!(view.approval_answer("/help")?.is_none());
        view.phase = Some(Phase::Idle);
        assert!(view.approval_answer("yes")?.is_none());
        assert_eq!(prepared.1, binding);
        Ok(())
    }

    #[test]
    fn a_reopened_view_is_busy_while_its_restored_turn_waits() -> Result<(), Failure> {
        // A reopened view sent nothing: only the committed phase says a
        // turn is open, so Esc and Ctrl+C reach it.
        let mut view = View::default();
        view.phase = Some(Phase::Waiting {
            deadline_unix_ms: None,
            input: false,
        });
        assert!(view.busy());
        assert_eq!(view.status(), "working · Esc or Ctrl+C interrupts");
        for phase in [
            Phase::Running,
            Phase::WaitingApproval {
                id: IntentId::try_from("approval")?,
                binding: None,
            },
            Phase::WaitingHost {
                host: "host".into(),
            },
            Phase::Waiting {
                deadline_unix_ms: Some(1),
                input: false,
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

// Reopened commands and follows compare against the last displayed layout.
// Compare content, since an unrelated file commit also changes file revisions.
// An unchanged layout preserves the selected page and its retained read.
fn apply_session_layout<F>(
    ui: &session::Session,
    previous: &mut Option<[u8; 32]>,
    overlay: &mut Option<(&'static str, Vec<Line<'static>>)>,
    queries: &mut QueryController<query::Source, query::Context>,
    pending: std::pin::Pin<&mut Option<F>>,
    input: &PromptInput,
) -> Option<Option<String>> {
    if *previous == ui.layout_digest() {
        return None;
    }
    *previous = ui.layout_digest();
    Some(apply_open_view(
        ui.open_view(),
        overlay,
        queries,
        pending,
        &ui.catalog,
        input,
    ))
}

/// Projects the session's saved view onto the terminal. Total over every
/// view name: `layout.json` is shared with other clients and the shared
/// `view_changes` writer, so a view this terminal cannot render closes the
/// panel and returns a notice instead of ending the session.
fn apply_open_view<F>(
    selected: Option<&str>,
    overlay: &mut Option<(&'static str, Vec<Line<'static>>)>,
    queries: &mut QueryController<query::Source, query::Context>,
    pending: std::pin::Pin<&mut Option<F>>,
    catalog: &CommandCatalog,
    input: &PromptInput,
) -> Option<String> {
    *overlay = None;
    match selected {
        Some("cost") => queries.select(query::Source::Cost, pending),
        Some("diff") => queries.select(query::Source::Diff(None), pending),
        Some("history") => queries.select(query::Source::History(None), pending),
        Some("shortcuts") => {
            queries.close(pending);
            *overlay = Some(("Help", View::help(catalog, input)));
        }
        None => queries.close(pending),
        Some(other) => {
            queries.close(pending);
            return Some(format!(
                "The saved {other:?} view is not available in this terminal"
            ));
        }
    }
    None
}
