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
        Sender, TurnInput, TurnOutcome,
    },
    client::commands::{CommandCatalog, CommandQuery, CommandRequest, CommandResolution},
    executor::ToolCard,
    interaction::{ApprovalBinding, ApprovalVerdict},
    tui::{
        Action, AppAction, BlockKind, Editor, InputEffect, PromptInput, QueryController, QueryPage,
        Screen, Thinking, TranscriptRows, Viewport,
        config::Token,
        context_summary, feedback_summary,
        key_event::{InputEvent, KeyCode, KeyEventKind},
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
use futures::StreamExt as _;
use std::{sync::Arc, time::Instant};

type Failure = Box<dyn std::error::Error>;
use acyclic_harness::completion::Exit;
mod capabilities;
mod diff;
mod query;
mod session;
mod tree;

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
        for (sequence, entry) in (from..to).zip(conversation.entries(from, to).await?) {
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
                page.blocks
                    .extend(turn(&conversation, sequence, &entry).await?);
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
    sequence: u64,
    entry: &ConversationEntry,
) -> acyclic_harness::Result<Vec<(BlockKind, String)>> {
    if entry.input.context().is_some() {
        return Ok(Vec::new());
    }
    let turn = conversation.turn_at(sequence, entry).await?;
    let mut blocks = Vec::new();
    blocks.extend(turn.prompt.map(|text| prompt(&entry.input, text)));
    blocks.extend(work(turn.tools, turn.answer));
    if entry.outcome == TurnOutcome::Cancelled {
        blocks.push((BlockKind::Answer, CANCELLED.into()));
    }
    Ok(blocks)
}

/// Input attribution comes from the committed envelope, never message text.
fn prompt(input: &TurnInput, text: String) -> (BlockKind, String) {
    match input {
        TurnInput::Message {
            from: Sender::Agent { path },
            id,
            re,
            ..
        } => {
            let reply = re
                .as_ref()
                .map_or_else(String::new, |id| format!(" · re {}", id.as_str()));
            (
                BlockKind::Peer,
                format!("Peer {path} · {}{reply}\n{text}", id.as_str()),
            )
        }
        _ => (BlockKind::Prompt, text),
    }
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
                card.text(),
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

/// The same live projection responds to deltas and settings-file changes.
fn live_rows(kind: LiveKind, live: &str, input: &PromptInput) -> Vec<Line<'static>> {
    if kind == LiveKind::Answer
        || (kind == LiveKind::Reasoning && !input.editor.settings().reasoning)
    {
        return Vec::new();
    }
    let style = if kind == LiveKind::Reasoning {
        input
            .theme
            .token(Token::Muted)
            .add_modifier(Modifier::ITALIC)
    } else {
        Style::default()
    };
    let mut rows = live
        .lines()
        .rev()
        .take(OUTPUT_ROWS)
        .map(|line| Line::styled(line.to_owned(), style))
        .collect::<Vec<_>>();
    rows.reverse();
    rows
}
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
    async fn cancel(&mut self, conversation: &Conversation) -> Result<(), Failure> {
        self.cancelling = conversation.cancel().await?;
        Ok(())
    }
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
    /// Explicit terminal-exit choice; quitting otherwise only detaches.
    StopOrDetach,
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
    /// Retain one requested merge identity across transport uncertainty and
    /// conflict resolution; a redraw or repeated command cannot replace it.
    merge_operation: Option<acyclic_harness::OperationId>,
    transcript: TranscriptRows,
    /// Sent prompts not yet settled, by inbox sequence.
    pending: Vec<(u64, String)>,
    /// Sent compactions, rewinds and clears not yet settled, by inbox
    /// sequence.
    checkpoints: Vec<u64>,
    /// Inputs below this observed frontier cannot become pending again when
    /// an uncertain command later replays its original delivery receipt.
    settled_before: u64,
    /// A cancel's exact input and announced conversation tail; the marker waits
    /// for those entries, so their original partial output remains visible.
    stopped: Option<(u64, u64)>,
    /// A cancelled entry already marked its turn's stop.
    cancel_marked: bool,
    /// The authored session title, projected only for this attached view.
    title: String,
    /// The running turn's answer while it streams.
    reply: String,
    phase: Option<Phase>,
    /// A cancel was sent and its turn has not stopped yet.
    cancelling: Option<u64>,
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
    feedback_scroll: usize,
    /// A command's result or refusal, until the next prompt.
    notice: Option<String>,
    /// Original waiting-status identity of the action displayed by the pager.
    approval_status: Option<u64>,
    /// Exact immutable action whose final rows were painted.
    approval_shown: Option<acyclic_harness_client::PendingApproval>,
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

    async fn restore_phase(
        &mut self,
        conversation: &Conversation,
        phase: Phase,
    ) -> Result<(), Failure> {
        if !matches!((&phase, &self.approval_shown),
            (Phase::WaitingApproval { id, binding: Some(binding) }, Some(shown))
            if id == &shown.id && binding == &shown.binding)
        {
            self.approval_shown = None;
        }
        self.approval_status = match &phase {
            Phase::WaitingApproval { id, binding } => conversation
                .pending_approval(id)
                .await?
                .filter(|pending| Some(&pending.binding) == binding.as_ref())
                .map(|pending| pending.status),
            _ => None,
        };
        self.phase = Some(phase);
        Ok(())
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
                let turn = conversation
                    .turn_at(
                        seen.checked_sub(1)
                            .ok_or("turn has no committed sequence")?,
                        &entry,
                    )
                    .await?;
                if let Some(text) = sent.or(turn.prompt) {
                    let (kind, text) = prompt(&entry.input, text);
                    self.transcript.push(kind, &text);
                }
                for (kind, text) in work(turn.tools, turn.answer) {
                    self.transcript.push(kind, &text);
                }
                self.reply.clear();
                if cancelled {
                    self.transcript.push(BlockKind::Answer, CANCELLED);
                    self.outcome = Outcome::Cancelled;
                    // It marks the stop a cancel in flight would otherwise mark.
                    self.cancel_marked = self.cancelling == Some(entry.inbox)
                        || self.stopped.is_some_and(|(input, _)| input == entry.inbox);
                } else if let TurnOutcome::Failed { message } = &entry.outcome {
                    // The turn failed; the conversation stays open.
                    self.failed(conversation, message);
                } else {
                    self.outcome = Outcome::Settled;
                }
                if self.cancelling == Some(entry.inbox) {
                    self.cancelling = None;
                }
                self.ring = true;
                self.stop(seen);
            }
            Event::Delta => return Ok(()),
            Event::Status(status) => {
                if let Some(input) = status.cancelled_input {
                    self.stopped = Some((input, status.conversation));
                    if self.cancelling == Some(input) {
                        self.cancelling = None;
                        if matches!(self.phase, Some(Phase::WaitingApproval { .. })) {
                            self.notice =
                                Some(format!("task {} failed: cancelled", conversation.task()));
                        }
                    }
                }
                // A settled agent consumed every request before its cursor:
                // a context request it passed without an entry was dropped
                // (cancelled) or failed, and is no longer work.
                if matches!(status.phase, Phase::Idle | Phase::Done { .. }) {
                    self.observe_settled_before(status.cursor);
                }
                let failure = match &status.phase {
                    Phase::Done {
                        outcome: AgentOutcome::Failed { message },
                    } => Some(message.clone()),
                    Phase::Done {
                        outcome: AgentOutcome::FailedDiagnostic { diagnostic },
                    } => {
                        let page = conversation
                            .diagnostic_page(diagnostic, 0, acyclic_stream::MAX_RECORD_BYTES as u64)
                            .await?;
                        let mut text = page.text;
                        if page.end < page.total {
                            text.push_str(&format!(
                                "\n\nComplete diagnostic: {} (next byte {}, total {}).",
                                diagnostic.query_path(),
                                page.end,
                                page.total
                            ));
                        }
                        Some(text)
                    }
                    _ => None,
                };
                if let Some(message) = failure {
                    self.settle_pending();
                    self.failed(conversation, &message);
                } else if matches!(status.phase, Phase::Idle | Phase::Done { .. }) {
                    // Idle acknowledges settlement, not that our cancellation
                    // won. Only its matching status or cancelled entry marks it.
                    self.cancelling = None;
                    if self.stopped.is_none() {
                        // A prompt the cursor passed shows with its turn's entry.
                        self.pending.retain(|(inbox, _)| *inbox >= status.cursor);
                    }
                }
                self.pending_dirty = true;
                self.restore_phase(conversation, status.phase).await?;
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
        if let CommandRequest::Peers { enabled } = &request {
            acyclic_harness::local::sessions::set_peers(conversation.connection(), &id, *enabled)
                .await?;
            self.notice = Some(
                if *enabled {
                    "Peer sessions on"
                } else {
                    "Peer sessions off"
                }
                .into(),
            );
            return Ok(());
        }
        if matches!(request, CommandRequest::Merge {}) {
            let operation = *self
                .merge_operation
                .get_or_insert_with(acyclic_harness::OperationId::new);
            match acyclic_harness::local::sessions::merge(
                conversation.connection(),
                &operation.to_string(),
            )
            .await
            {
                Ok(commit) => {
                    self.merge_operation = None;
                    self.notice = Some(commit.map_or_else(
                        || "Session already integrated".into(),
                        |commit| format!("Merged into checkout: {commit}"),
                    ));
                }
                Err(error) => self.notice = Some(error.to_string()),
            }
            return Ok(());
        }
        if let CommandRequest::Pause { scope } | CommandRequest::Resume { scope } = &request {
            let paused = matches!(request, CommandRequest::Pause { .. });
            match scope {
                acyclic_harness::client::commands::CommandTaskScope::Subtree => {
                    acyclic_harness::local::sessions::pause_subtree(
                        conversation.connection(),
                        conversation.connection().root().clone(),
                        paused,
                    )
                    .await?;
                }
                acyclic_harness::client::commands::CommandTaskScope::All => {
                    acyclic_harness::local::sessions::pause_all(conversation.connection(), paused)
                        .await?;
                }
            }
            self.notice = Some(
                if paused {
                    "Session paused"
                } else {
                    "Session resumed"
                }
                .into(),
            );
            return Ok(());
        }
        let inbox = match &request {
            CommandRequest::Compact { instructions } => {
                let instructions = instructions.trim();
                conversation
                    .compact(&id, (!instructions.is_empty()).then_some(instructions))
                    .await?
            }
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
    /// shown (`seen` is past them): only the prompt that it cancelled,
    /// and its mark unless a cancelled entry already made it. The text a
    /// stopped turn saved is its entry's, never the streamed preview's.
    fn stop(&mut self, seen: u64) {
        let Some((input, announced)) = self.stopped else {
            return;
        };
        if seen < announced {
            return;
        }
        self.stopped = None;
        let mut dropped = Vec::new();
        self.pending.retain(|(inbox, prompt)| {
            let kept = *inbox != input;
            if !kept {
                dropped.push(prompt.clone());
            }
            kept
        });
        for prompt in dropped {
            self.transcript.push(BlockKind::Prompt, &prompt);
        }
        if !std::mem::take(&mut self.cancel_marked) {
            if !self.phase.as_ref().is_some_and(Phase::in_turn) {
                self.reply.clear();
            }
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
        for command in capabilities::commands(catalog) {
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
        if !self.title.is_empty() {
            spans.push(Span::raw(format!(" · {}", self.title)));
        }
        if self.outcome == Outcome::Cancelled && !self.busy() {
            spans.push(Span::raw(" ■ STOPPED"));
        }
        Line::from(spans)
    }

    fn status(&self) -> String {
        if matches!(self.prompt, Some(Prompt::StopOrDetach)) {
            return "Stop or detach? s stops this turn; d or Ctrl+D detaches; Esc stays.".into();
        }
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
            _ => "Enter sends · Ctrl+F finds prompt text · Ctrl+S sessions · Ctrl+D stop or detach"
                .into(),
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

/// Runs the terminal view; `resume` reopens the last conversation's recent
/// turns, paged in from the stream as they show, otherwise the view starts
/// empty.
enum Next {
    Exit,
    Picker,
    New,
    HistoryBranch,
}

pub async fn run(
    mut conversation: Arc<Conversation>,
    inline: bool,
    mut resume: bool,
    mut terminal: Option<Tty>,
    price_file: Option<String>,
    questions: bool,
) -> Result<(), Failure> {
    loop {
        match run_current(
            Arc::clone(&conversation),
            inline,
            resume,
            terminal.take(),
            price_file.clone(),
        )
        .await?
        {
            Next::Exit => return Ok(()),
            Next::New => {
                let connection = acyclic_harness::local::sessions::create(
                    conversation.connection(),
                    &acyclic_harness::OperationId::new().to_string(),
                    Some(questions),
                )
                .await?;
                conversation = Conversation::open(connection);
                resume = false;
            }
            Next::Picker => {
                let (connection, owned) = super::sessions::picker(
                    conversation.connection().clone(),
                    None,
                    None,
                    price_file.clone(),
                    questions,
                )
                .await?;
                conversation = Conversation::open(connection);
                terminal = owned;
                resume = true;
            }
            Next::HistoryBranch => {
                let (connection, owned) =
                    super::sessions::history_picker(conversation.connection().clone(), None)
                        .await?;
                conversation = Conversation::open(connection);
                terminal = owned;
                resume = true;
            }
        }
    }
}

async fn run_current(
    mut conversation: Arc<Conversation>,
    inline: bool,
    resume: bool,
    terminal: Option<Tty>,
    price_file: Option<String>,
) -> Result<Next, Failure> {
    let title = conversation
        .agent_file_previews(
            vec!["ui/title".into()],
            None,
            acyclic_harness::client::MAX_READ_BYTES,
        )
        .await?;
    let mut title_future = std::pin::pin!(session::follow_title(
        Arc::clone(&conversation),
        title.generation.clone()
    ));
    let root = Arc::clone(&conversation);
    let mut settlement_queries = futures::stream::FuturesUnordered::new();
    let mut settlement_replies = Vec::new();
    let mut sidebar = tree::Sidebar::new(&conversation)?;
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
    let mut keys = acyclic_harness::tui::term::input_events(tty)?;
    // Its settled turns from the tail on, its phase and, while it works, the
    // newest tool call's output or its streaming answer or reasoning. A
    // resumed view replays its newest turn, whose outcome is the session's,
    // and pages the turns before it in as it shows them.
    let replayed_through = if resume {
        conversation.settled().await?
    } else {
        0
    };
    let from = resume.then_some(replayed_through.saturating_sub(1));
    let (mut events, phase) = conversation.observe_with_phase(from).await?;
    let mut view = View {
        title: session::title(&title),
        top: events.seen(),
        floor: if resume { 0 } else { events.seen() },
        ..View::default()
    };
    view.restore_phase(&conversation, phase).await?;
    if view.busy() {
        view.pending = conversation.pending_inputs().await?;
    }
    let mut older_future = std::pin::pin!(None);
    let mut output_rows = Vec::new();
    let mut input = PromptInput::default();
    ui.config
        .apply(&mut input, &profile)
        .map_err(|errors| format!("UI configuration: {errors:?}"))?;
    view.transcript.set_theme(input.theme);
    view.transcript
        .set_tool_details(input.editor.settings().tool_details);
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
    let tree_marker = [Line::raw("")];
    loop {
        let mut awaiting_paint = Vec::new();
        for (id, settled) in settlement_replies.drain(..) {
            let settled: acyclic_harness::local::agent_conversation::InputSettlement = settled;
            if conversation.agent() == root.agent() && events.seen() < settled.conversation {
                awaiting_paint.push((id, settled));
            } else {
                terminal.application_reply(&format!(
                    "acyclic;settled;{id};{}",
                    serde_json::to_string(&settled.phase)?
                ))?;
            }
        }
        settlement_replies = awaiting_paint;
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
        let navigation = sidebar.navigation();
        let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let size: Size = size()?;
        usage_page.wrap(size.width);
        let busy = view.busy();
        input.busy = busy;
        let shown = ui.tree.unwrap_or_else(|| sidebar.has_children());
        // The same layout as painting owns row credit, including a growing
        // prompt, the usage footer and a running tool's terminal strip.
        let tree_area = Screen {
            tree: if shown { &tree_marker } else { &[] },
            usage: if usage_stopped.is_some() {
                &usage_unavailable[..]
            } else {
                &usage_page.lines
            },
            terminal: if busy { &output_rows[..] } else { &[] },
            ..Screen::default()
        }
        .areas(Rect::from((Position::ORIGIN, size)), &input)
        .tree;
        sidebar.frame(now, tree_area);
        let tree_shown = ui.tree.unwrap_or_else(|| sidebar.has_children());
        let tree_rows = sidebar.rows(root.agent(), tree_shown && size.width >= 80, size.width / 3);
        let transcript_width = size
            .width
            .saturating_sub(if tree_shown && size.width >= 80 {
                size.width / 3
            } else {
                0
            });
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
        if let Some(Phase::WaitingUser { path }) = &view.phase
            && queries.source().is_none()
            && overlay.is_none()
            && view.prompt.is_none()
        {
            queries.select(query::Source::Question(path.clone()), query_future.as_mut());
            overlay_top = 0;
        }
        if matches!(queries.source(), Some(query::Source::Question(_)))
            && !matches!(view.phase, Some(Phase::WaitingUser { .. }))
        {
            queries.close(query_future.as_mut());
        }
        let feedback = matches!(view.prompt, Some(Prompt::Feedback));
        if let Some(panel) = queries.page.as_mut() {
            let area = Rect::from((Position::ORIGIN, size));
            let columns = Screen::default().areas(area, &input).overlay_columns();
            if let Some(diff) = panel.context.diff.as_mut()
                && let Some(lines) = diff.render(columns, size.width >= 160, input.theme)
            {
                panel.replace_source(lines);
            }
            panel.wrap(columns);
        }
        let (areas, page) = {
            let status = Line::raw(
                capabilities::selected(&ui.catalog, input.editor.text())
                    .filter(|_| view.prompt.is_none())
                    .map_or_else(|| view.status(), str::to_owned),
            );
            let mut header = view.header();
            header
                .spans
                .push(Span::raw(format!(" · {}", conversation.agent().id())));
            view.rows(transcript_width.saturating_sub(1));
            let (transcript, links, code) = view.transcript.presentation();
            let mut screen = Screen {
                header,
                transcript,
                links,
                code,
                tree: &tree_rows,
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
            if feedback {
                let viewport = screen.areas(area, &input).transcript;
                ui.feedback_page.wrap(viewport.width.saturating_sub(2));
                view.feedback_scroll = view.feedback_scroll.min(
                    ui.feedback_page
                        .lines
                        .len()
                        .saturating_sub(usize::from(viewport.height.saturating_sub(2))),
                );
                screen.feedback_rows = &ui.feedback_page.lines;
                screen.feedback_scroll = view.feedback_scroll;
            }
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
                Arc::clone(&root),
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
        let tree_deadline = sidebar.source.next_deadline();
        tokio::select! {
            Some((id, settled)) = settlement_queries.next(), if !settlement_queries.is_empty() => {
                let settled: acyclic_harness::Result<acyclic_harness::local::agent_conversation::InputSettlement> = settled;
                settlement_replies.push((id, settled?));
            }
            () = futures::future::poll_fn(|cx| sidebar.files.poll_changed(cx)) => {}
            () = futures::future::poll_fn(|cx| sidebar.source.poll_changed(now, cx)) => {}
            () = async {
                if let Some(deadline) = tree_deadline {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(
                        start + std::time::Duration::from_millis(deadline)
                    )).await;
                } else {
                    std::future::pending().await
                }
            } => {}
            title = &mut title_future => {
                let title = title?;
                view.title = session::title(&title);
                title_future.set(session::follow_title(Arc::clone(&conversation), title.generation));
            }
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
                            usage_page = QueryPage::new(vec![context_summary(query.model(), query.context(), query.open_children()), cost, feedback_summary(&query.tools().feedback)], None, false);
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
                    // Another client can start a turn while this view is idle.
                    // Refresh once when its live output becomes visible.
                    if !view.busy() && !events.live().1.is_empty() {
                        view.restore_phase(&conversation, conversation.phase().await?).await?;
                    }
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
                // The initial query already includes the selected replay prefix.
                // Only newly settled turns can change its selected source.
                if settled && events.seen() > replayed_through {
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
                output_rows = live_rows(kind, live, &input);
            }
            batch = async {
                if let Some(agent) = navigation {
                    Some(Ok((Some(agent), Vec::new())))
                } else {
                    keys.next().await.map(|batch| batch.map(|events| (None, events)))
                }
            } => {
                previous_layout = ui.layout_digest();
                let (navigation, batch) = batch.ok_or("terminal input ended")??;
                for event in navigation.into_iter().map(Err).chain(batch.into_iter().map(Ok)) {
                    if let Ok(InputEvent::SettlementQuery(id)) = &event {
                        let id = *id;
                        let root = Arc::clone(&root);
                        settlement_queries.push(async move { (id, root.wait_current_input().await) });
                        continue;
                    }
                    let intent = match &event {
                        Err(agent) => Some(tree::Intent::Attach(agent.clone())),
                        Ok(event) if overlay.is_none() && queries.source().is_none() => sidebar.event(event, &input.keymap, areas.tree),
                        Ok(_) => None,
                    };
                    if let Some(intent) = intent {
                        let attach = match intent {
                            tree::Intent::Toggle => {
                                let shown = !ui.tree.unwrap_or(tree_shown);
                                ui.show_tree(&conversation, shown).await?;
                                sidebar.focused = shown;
                                files_changed = true;
                                None
                            }
                            tree::Intent::Attach(agent) => Some((agent, None)),
                            tree::Intent::Parent => conversation.agent().parent()
                                .filter(|agent| root.agent().contains(agent)).map(|agent| (agent, None)),
                            tree::Intent::History(direction) => sidebar.history_target(direction),
                            tree::Intent::Sibling(forward) => {
                                sidebar.sibling(conversation.agent(), forward);
                                None
                            }
                            tree::Intent::Diff(agent) => {
                                queries.select(query::Source::AgentDiff(agent, None), query_future.as_mut());
                                None
                            }
                            tree::Intent::Merge(agent) => {
                                queries.select(query::Source::Merge(agent, acyclic_harness::OperationId::new().to_string()), query_future.as_mut());
                                None
                            }
                            tree::Intent::Discard(agent) => {
                                queries.select(query::Source::Discard(agent, acyclic_harness::OperationId::new().to_string()), query_future.as_mut());
                                None
                            }
                            tree::Intent::Wait(agent) => {
                                queries.select(query::Source::Wait(agent), query_future.as_mut());
                                None
                            }
                            tree::Intent::Input | tree::Intent::Consumed => None,
                        };
                        if let Some((agent, history)) = attach {
                            let opening = async {
                                let selected = root.select(agent.clone()).await?;
                                let from = selected.settled().await?.saturating_sub(1);
                                let (events, phase) = selected.observe_with_phase(Some(from)).await?;
                                let session = session::Session::open(&selected).await?;
                                let title = selected.agent_file_previews(vec!["ui/title".into()], None, acyclic_harness::client::MAX_READ_BYTES).await?;
                                let mut view = View { title: session::title(&title), top: events.seen(), floor: 0, pending_dirty: true, tail_dirty: true, ..View::default() };
                                view.restore_phase(&selected, phase).await?;
                                if view.busy() {
                                    view.pending = selected.pending_inputs().await?;
                                }
                                Ok::<_, Failure>((selected, events, session, title, view))
                            }.await;
                            let (selected, selected_events, session, title, selected_view) = match opening {
                                Ok(opened) => opened,
                                Err(error) => {
                                    sidebar.cancel_navigation();
                                    view.notice = refusal(format_args!("Attach unavailable: {error}"));
                                    continue;
                                }
                            };
                            sidebar.attached(&agent, history);
                            events = selected_events;
                            conversation = selected;
                            view = selected_view;
                            title_future.set(session::follow_title(Arc::clone(&conversation), title.generation));
                            ui = session;
                            queries.close(query_future.as_mut());
                            overlay = None;
                            older_future.set(None);
                            output_rows.clear();
                            usage_stopped = None;
                            usage_page = QueryPage::new(vec![Line::raw("Loading usage…")], None, false);
                            usage_prices = Arc::clone(&ui.prices);
                            usage_future.set(Some(usage(conversation.usage_query_with_prices(Arc::clone(&usage_prices)))));
                            sidebar.focused = false;
                            input.editor = Editor::default();
                            unsent = None;
                            files_changed = true;
                        }
                        continue;
                    }
                    let Ok(event) = event else { continue };
                    if input.editor.text().is_empty() && view.prompt.is_none() && overlay.is_none()
                        && let InputEvent::Key(key) = &event
                        && key.kind != KeyEventKind::Release
                        && key.modifiers.is_empty()
                        && let Some(diff) = queries.page.as_mut().and_then(|page| page.context.diff.as_mut())
                    {
                        let consumed = match key.code {
                            KeyCode::Tab => diff.select_hunk(false),
                            KeyCode::Char(' ') => diff.toggle_hunk(),
                            _ => false,
                        };
                        if consumed { overlay_top = 0; continue; }
                    }
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
                                        let saved = ui.save_feedback(&conversation, &text).await;
                                        files_changed = true;
                                        match saved {
                                            Ok(()) => {
                                                view.notice = Some(format!(
                                                    "Feedback saved to feedback.json ({} this session)",
                                                    ui.feedback.len()
                                                ));
                                            }
                                            Err(error) => {
                                                view.notice = Some(format!("Feedback save not confirmed: {error}"));
                                                view.prompt = Some(Prompt::Feedback);
                                                input.editor = Editor::new(ui.feedback_draft().unwrap_or(&text));
                                            }
                                        }
                                        continue;
                                    }
                                    Some(Prompt::StopOrDetach) => {
                                        match text.trim().to_ascii_lowercase().as_str() {
                                            "s" | "stop" => { view.cancel(&conversation).await?; return Ok(Next::Exit); }
                                            "d" | "detach" | "" => return view.exit().map(|()| Next::Exit),
                                            _ => { view.prompt = Some(Prompt::StopOrDetach); }
                                        }
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
                                    input.editor.set_text(&text, now);
                                    continue;
                                }
                                match view.approval_answer(&text) {
                                    Ok(Some((id, binding, approved))) => {
                                        if let Err(error) = conversation.decide_with_binding(&id, binding, approved).await {
                                            view.notice = refusal(error);
                                            input.editor.set_text(&text, now);
                                        }
                                        continue;
                                    }
                                    Err(reason) => {
                                        view.notice = refusal(reason);
                                        input.editor.set_text(&text, now);
                                        continue;
                                    }
                                    Ok(None) => {}
                                }
                                if let Some(reason) = capabilities::selected(&ui.catalog, &text) {
                                    view.notice = Some(reason.into());
                                    input.editor.set_text(&text, now);
                                    continue;
                                }
                                match ui.catalog.resolve(&text, u64::MAX) {
                                    Ok(None) => {}
                                    Ok(Some(CommandResolution::Submit { request: CommandRequest::New {} })) => {
                                        return Ok(Next::New);
                                    }
                                    Ok(Some(CommandResolution::Submit { request: CommandRequest::Rewind {} })) => {
                                        return Ok(Next::HistoryBranch);
                                    }
                                    Ok(Some(CommandResolution::Select { query: CommandQuery::Sessions })) => {
                                        return Ok(Next::Picker);
                                    }
                                    Ok(Some(CommandResolution::Submit { request: CommandRequest::Quit {} })) => {
                                        return view.exit().map(|()| Next::Exit);
                                    }
                                    Ok(Some(resolution @ (CommandResolution::Submit { request: CommandRequest::Theme { .. } } | CommandResolution::Select { query: CommandQuery::Details | CommandQuery::Reasoning }))) => {
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
                                        view.feedback_scroll = 0;
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
                                        input.editor.set_text(&text, now);
                                        continue;
                                    }
                                }
                                if let Some((path, generation, blocks)) = queries.page.as_ref().and_then(|page| page.context.question.as_ref()) {
                                    let reply = acyclic_harness::client::ui::Reply {
                                        path: acyclic_harness::client::ui::UiPath::new(path.clone())?,
                                        generation: generation.clone(),
                                        answers: Default::default(),
                                        text: text.clone(),
                                    };
                                    let command = reply.command(conversation.agent().clone(), acyclic_harness::agent::IntentId::try_from(acyclic_harness::OperationId::new().to_string())?, blocks)?;
                                    match conversation.connection().execute(&command).await {
                                        Ok(_) => { queries.close(query_future.as_mut()); }
                                        Err(error) => { view.notice = refusal(error); input.editor.set_text(&text, now); }
                                    }
                                    continue;
                                }
                                // A refused prompt, such as an unknown /name, stays
                                // in the editor with its reason. Sending the same text
                                // again reuses its message id, so a send whose commit
                                // was uncertain replays exactly and never commits twice.
                                let id = match unsent.take() {
                                    Some((id, sent)) if sent == text => id,
                                    _ => acyclic_harness::OperationId::new().to_string(),
                                };
                                let target = acyclic_harness::client::ui::input_target(sidebar.source.view.reader_ref(), conversation.agent());
                                let recipient = if &target == conversation.agent() { Arc::clone(&conversation) } else { root.select(target).await? };
                                let inbox = match recipient.send(&id, &text).await {
                                    Ok(inbox) => inbox,
                                    Err(error) => {
                                        view.notice = refusal(error);
                                        input.editor.set_text(&text, now);
                                        unsent = Some((id, text));
                                        continue;
                                    }
                                };
                                if recipient.agent() != conversation.agent() {
                                    view.notice = Some(format!("Sent to orchestrator {}", recipient.agent().id()));
                                    continue;
                                }
                                view.pending.push((inbox, text));
                                view.back = 0;
                                view.pending_dirty = true;
                            }
                            InputEffect::Host(Action::Escape) if view.prompt.is_some() => {
                                view.prompt = None;
                            }
                            InputEffect::Interrupt | InputEffect::Host(Action::Escape)
                                if matches!(queries.source(), Some(query::Source::Approval { .. })) =>
                            {
                                view.cancel(&conversation).await?;
                            }
                            InputEffect::Host(Action::Escape) if overlay.is_some() || queries.source().is_some() => {
                                match ui.select(&conversation, None).await {
                                    Ok(()) => {
                                        files_changed = true;
                                    }
                                    Err(error) => view.notice = Some(error.to_string()),
                                }
                            }
                            InputEffect::Interrupt | InputEffect::Host(Action::Escape) => {
                                // No output need have arrived from a peer's
                                // turn yet; a cached idle phase cannot discard
                                // an explicit interrupt of accepted input.
                                if !view.busy() {
                                    view.restore_phase(&conversation, conversation.phase().await?).await?;
                                }
                                if view.busy() {
                                    view.cancel(&conversation).await?;
                                } else if matches!(effect, InputEffect::Interrupt) {
                                    queries.close(query_future.as_mut());
                                    overlay_top = 0;
                                }
                            }
                            InputEffect::Quit if matches!(view.prompt, Some(Prompt::StopOrDetach)) => return view.exit().map(|()| Next::Exit),
                            InputEffect::Quit => { view.prompt = Some(Prompt::StopOrDetach); }
                            InputEffect::Scroll(rows) if matches!(view.prompt, Some(Prompt::Feedback)) => {
                                ui.feedback_page.page_boundary(&mut view.feedback_scroll, rows, areas.transcript.height.saturating_sub(2));
                            }
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
                            InputEffect::Host(Action::App(AppAction::Sessions)) => return Ok(Next::Picker),
                            InputEffect::Host(Action::App(AppAction::NewSession)) => return Ok(Next::New),
                            InputEffect::Host(Action::App(AppAction::Quit)) => return view.exit().map(|()| Next::Exit),
                            InputEffect::Host(Action::App(AppAction::Find)) => {
                                view.find(input.editor.text(), usize::from(areas.transcript.height));
                            }
                            InputEffect::Host(Action::App(AppAction::Details)) => {
                                match ui.submit(&conversation, &CommandResolution::Select { query: CommandQuery::Details }).await {
                                    Ok(()) => files_changed = true,
                                    Err(error) => view.notice = Some(error.to_string()),
                                }
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
            view.transcript
                .set_tool_details(input.editor.settings().tool_details);
            let (kind, live) = events.live();
            output_rows = live_rows(kind, live, &input);
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
    async fn stop_targets_the_active_input_while_a_later_prompt_is_pending() -> Result<(), Failure>
    {
        use acyclic_harness::{
            LocalHarness,
            context::{CompactionPolicy, ModelContextCapacity},
            local::WorkspaceMounting,
            model::Model,
            test_support::{
                ScriptedEvent, ScriptedModelProvider, ScriptedTurn, local_harness_config,
            },
        };
        let directory = tempfile::tempdir()?;
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace)?;
        let (entered, entering) = tokio::sync::oneshot::channel();
        let gate = std::sync::Mutex::new(Some(entered));
        let provider = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Events(vec![ScriptedEvent::Pending {}]),
                ScriptedTurn::Text("later answer".into()),
                ScriptedTurn::Text("completed before cancellation".into()),
            ])
            .with_context_capacity(ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: Some(4096),
            })
            .with_dispatch_observer(move |_| {
                if let Some(entered) = gate.lock().unwrap().take() {
                    let _ = entered.send(());
                }
                Box::pin(async { Ok(()) })
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
                .without_default_tools()
                .feedback_plugin(false)
                .without_user_settings()
                .state_directory(directory.path().join("state"))
                .workspace_mounting(WorkspaceMounting::Physical)
                .compaction(CompactionPolicy::Disabled),
            )
            .await?,
        );
        let conversation = Conversation::open(harness.connect().await?);
        let mut events = conversation.observe(None).await?;
        let active = conversation.send("active", "keep running").await?;
        tokio::select! {
            entered = entering => entered?,
            settled = conversation.wait_current_input() => {
                return Err(format!("active input settled before dispatch: {settled:?}").into());
            }
        }
        let later = conversation.send("later", "later prompt").await?;
        let mut view = View {
            pending: vec![
                (active, "keep running".into()),
                (later, "later prompt".into()),
            ],
            ..View::default()
        };
        view.cancel(&conversation).await?;
        assert_eq!(view.cancelling, Some(active));
        let settlement = conversation.settle("later", later, &mut events).await?;
        assert!(
            matches!(
                &settlement,
                acyclic_harness::local::agent_conversation::Settlement::Reply(
                    acyclic_harness::agent::Reply::Answered { entry, .. }
                ) if entry.inbox == later && entry.outcome == TurnOutcome::Completed
            ),
            "later input did not complete independently: {settlement:?}"
        );
        if let acyclic_harness::local::agent_conversation::Settlement::Reply(
            acyclic_harness::agent::Reply::Answered {
                sequence, entry, ..
            },
        ) = &settlement
        {
            assert_eq!(
                conversation.output_text_at(*sequence, entry).await?,
                "later answer"
            );
        }
        let entries = conversation
            .entries(0, conversation.settled().await?)
            .await?;
        assert!(
            entries
                .iter()
                .any(|entry| entry.inbox == active && entry.outcome == TurnOutcome::Cancelled)
        );
        assert_eq!(provider.request_count(), 2);
        assert_eq!(conversation.cancel().await?, None);

        let mut completed_events = conversation.observe(None).await?;
        let completed = conversation
            .send("completion-wins", "finish successfully")
            .await?;
        conversation.wait_current_input().await?;
        // The view retained the selection before completion, but completion's
        // terminal fact committed before its control reached the actor.
        let mut completed_view = View {
            pending: vec![(completed, "finish successfully".into())],
            cancelling: Some(completed),
            phase: Some(Phase::Running),
            ..View::default()
        };
        conversation.cancel_input(completed).await?;
        conversation.wait_current_input().await?;
        let mut answered = false;
        let mut idle = false;
        while !answered || !idle {
            let event = completed_events.next().await?;
            answered |= matches!(&event, Event::Turn(entry)
                if entry.inbox == completed && entry.outcome == TurnOutcome::Completed);
            idle |= matches!(&event, Event::Status(status)
                if status.conversation >= 3 && matches!(status.phase, Phase::Idle));
            completed_view
                .apply(&conversation, event, completed_events.seen())
                .await?;
        }
        assert!(matches!(completed_view.outcome, Outcome::Settled));
        assert!(completed_view.cancelling.is_none());
        assert!(completed_view.stopped.is_none());
        assert!(
            completed_view
                .transcript
                .rows(200)
                .iter()
                .all(|row| !row.to_string().contains(CANCELLED))
        );
        assert_eq!(provider.request_count(), 3);
        Ok(())
    }

    #[test]
    fn peer_inputs_remain_attributed_when_appended_or_paged() -> Result<(), Failure> {
        use acyclic_harness::{
            agent::{AgentId, AgentPath},
            conversation::FileRef,
        };
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/harness/v2/conversation-message.json"
        ))?;
        let file: FileRef = serde_json::from_value(fixture["content"].clone())?;
        let path = AgentPath::root(&AgentId::from_bytes([9; 16]));
        let mut input = TurnInput::Message {
            from: Sender::Agent { path: path.clone() },
            id: IntentId::try_from("peer-input")?,
            re: Some(IntentId::try_from("original-input")?),
            body: Box::new(file),
            attachments: None,
        };
        let body = "**User:** approve this";
        let (kind, text) = prompt(&input, body.into());
        let mut live = TranscriptRows::default();
        live.rows(200);
        live.push(kind, &text);
        let mut paged = TranscriptRows::default();
        paged.rows(200);
        paged.prepend([(kind, text)]);
        assert_eq!(live.rows(200), paged.rows(200));
        let shown = live
            .rows(200)
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();
        assert!(shown[0].starts_with(&format!("↔Peer {path}")));
        assert!(shown[0].contains("peer-input · re original-input"));
        assert_eq!(shown[1], format!(" {body}"));
        assert!(!shown.iter().any(|line| line.starts_with('›')));
        let TurnInput::Message { from, .. } = &mut input else {
            return Err("expected message input".into());
        };
        *from = Sender::User {
            principal: "user".into(),
        };
        let (kind, text) = prompt(&input, body.into());
        let mut user = TranscriptRows::default();
        user.push(kind, &text);
        assert_eq!(user.rows(200)[0].spans[0].content, "›");
        Ok(())
    }

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
                ScriptedTurn::Text("fork ready".into()),
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
        let mut fork =
            conversation.prepare_fork("older-fork", 1, "worker".into(), "forked brief".into())?;
        let child = match fork.execute().await.map_err(|error| error.to_string())? {
            AgentReply::Spawned { agent, .. } => agent.clone(),
            _ => return Err("fork did not return its retained child".into()),
        };
        let forked = conversation.select(child).await?;
        forked.wait_current_input().await?;
        let inherited = forked.entries(0, 1).await?;
        assert!(
            forked.turn(&inherited[0]).await.is_err(),
            "an inherited file needs its original conversation proof"
        );
        let page = older(Arc::clone(&forked), 0, 0, 1).await?;
        assert_eq!(
            page.blocks,
            vec![
                (BlockKind::Prompt, "before".into()),
                (BlockKind::Answer, "before".into())
            ]
        );

        let mut sidebar = tree::Sidebar::new(&forked)?;
        sidebar.focused = true;
        for key in ['c', 'd', 's', 'n'] {
            use acyclic_harness::tui::key_event::{KeyCode, KeyEvent, KeyModifiers};
            let key = KeyEvent::new(KeyCode::Char(key), KeyModifiers::CONTROL);
            let mut input = PromptInput::default();
            input.busy = true;
            assert!(
                sidebar
                    .event(
                        &InputEvent::Key(key),
                        &input.keymap,
                        Rect::new(0, 0, 30, 12)
                    )
                    .is_none(),
                "application action was swallowed: {key:?}"
            );
            let effects = input.key(key, 0, 80);
            assert!(
                effects.iter().any(|effect| matches!(
                    effect,
                    InputEffect::Interrupt
                        | InputEffect::Quit
                        | InputEffect::Host(Action::App(
                            AppAction::Sessions | AppAction::NewSession
                        ))
                )),
                "application action did not reach its normal router: {effects:?}"
            );
        }
        {
            use acyclic_harness::tui::key_event::{KeyCode, KeyEvent, KeyModifiers};
            let marker = [Line::raw("tree")];
            let screen = Screen {
                tree: &marker,
                ..Screen::default()
            };
            let mut input = PromptInput::default();
            let wide = screen.areas(Rect::new(0, 0, 100, 24), &input).tree;
            let discard = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE);
            sidebar.focused = true;
            assert!(
                sidebar
                    .event(&InputEvent::Key(discard), &input.keymap, wide)
                    .is_some()
            );
            for (width, height) in [(79, 24), (100, 0), (100, 3)] {
                let hidden = screen.areas(Rect::new(0, 0, width, height), &input).tree;
                sidebar.focused = true;
                sidebar.frame(0, hidden);
                assert!(!sidebar.focused, "resize retained invisible tree focus");
                let toggle = KeyEvent::new(KeyCode::Char('t'), KeyModifiers::CONTROL);
                assert!(matches!(
                    sidebar.event(&InputEvent::Key(toggle), &input.keymap, hidden),
                    Some(tree::Intent::Toggle)
                ));
                // A toggle and typing can arrive in the same input batch,
                // before another frame has released the requested focus.
                sidebar.focused = true;
                assert!(
                    sidebar
                        .event(&InputEvent::Key(discard), &input.keymap, hidden)
                        .is_none()
                );
                assert!(!sidebar.focused);
                let _ = input.key(discard, 0, usize::from(width));
                assert_eq!(input.editor.text(), "x");
                input = PromptInput::default();
                sidebar.frame(0, wide);
                assert!(
                    !sidebar.focused,
                    "growing the terminal restored stale focus"
                );
            }
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
            3,
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
        assert!(view.approval_answer("no").is_err());
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
