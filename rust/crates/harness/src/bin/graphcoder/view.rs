//! The interactive terminal: one conversation's settled turns, its pending
//! prompts and its phase, over the shared prompt editor and screen widgets.
use acyclic_harness::local::{Event, agent_conversation::Conversation};
use acyclic_harness::{
    agent::{IntentId, Phase},
    tui::{
        Action, AppAction, Editor, InputEffect, PromptInput, Screen, TranscriptRows, Viewport,
        key_event::InputEvent,
        ratatui::{
            layout::{Position, Rect, Size},
            style::{Modifier, Style},
            text::Line,
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

#[derive(Default)]
struct View {
    transcript: TranscriptRows,
    /// Sent prompts not yet settled, by inbox sequence.
    pending: Vec<(u64, String)>,
    phase: Option<Phase>,
    /// The latest failed delivery, until the next status.
    failure: Option<String>,
    /// Rows scrolled back from the live tail.
    back: usize,
    width: u16,
    pending_dirty: bool,
    search: Option<(String, usize)>,
}

impl View {
    fn busy(&self) -> bool {
        !self.pending.is_empty() || matches!(self.phase, Some(Phase::Running))
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
                let (prompt, answer) = conversation.turn(&entry).await?;
                let sent = self
                    .pending
                    .iter()
                    .position(|(pending, _)| *pending == entry.inbox)
                    .map(|at| self.pending.remove(at).1);
                if let Some(text) = sent.or(prompt) {
                    self.transcript.push(true, &text);
                }
                self.transcript.push(false, &answer);
                self.pending_dirty = true;
            }
            Event::Failed(error) => self.failure = Some(error),
            Event::Output => return Ok(()),
            Event::Status(status) => {
                self.failure = None;
                if matches!(status.phase, Phase::Idle | Phase::Done { .. }) {
                    let before = self.pending.len();
                    self.pending.retain(|(inbox, _)| *inbox >= status.cursor);
                    self.pending_dirty |= before != self.pending.len();
                }
                self.phase = Some(status.phase);
            }
        }
        Ok(())
    }

    /// Append and transient-prompt changes leave settled rows wrapped once.
    fn rows(&mut self, width: u16) -> &[Line<'static>] {
        if self.pending_dirty || self.width != width {
            self.width = width;
            self.transcript.rows(width);
            self.transcript
                .pending(self.pending.iter().map(|(_, text)| text.as_str()));
            self.pending_dirty = false;
        }
        self.transcript.rows(width)
    }

    fn status(&self) -> String {
        match (&self.phase, self.approval()) {
            (_, Some(id)) => format!(
                "approval {} requested: submit y to approve, n to deny",
                id.as_str()
            ),
            (Some(Phase::Done { outcome }), _) => format!("conversation ended: {outcome:?}"),
            _ if self.failure.is_some() => format!(
                "agent turn failed: {}",
                self.failure.as_deref().unwrap_or_default()
            ),
            _ if self.busy() => "working · Esc or Ctrl+C interrupts".into(),
            _ if self.transcript.truncated() => {
                "earlier transcript omitted · Ctrl+F finds prompt text".into()
            }
            _ => "Enter sends · Ctrl+F finds prompt text · Ctrl+D quits".into(),
        }
    }
}

fn terminal_events(tty: Tty) -> LocalBoxStream<'static, std::io::Result<Vec<InputEvent>>> {
    futures::stream::try_unfold(tty, |mut tty| async move {
        loop {
            let (reader, events) = tokio::task::spawn_blocking(move || {
                let deadline = Instant::now() + std::time::Duration::from_secs(86_400);
                let events = tty.read_events(deadline);
                (tty, events)
            })
            .await
            .map_err(std::io::Error::other)?;
            tty = reader;
            let events = events?;
            if !events.is_empty() {
                return Ok(Some((events, tty)));
            }
        }
    })
    .boxed_local()
}

pub async fn run(conversation: Arc<Conversation>, inline: bool) -> Result<u8, Failure> {
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
    let mut keys = terminal_events(tty);
    let from = conversation.settled().await?.saturating_sub(HISTORY);
    // Its settled turns, its phase and, while it works, the newest tool
    // call's output.
    let mut events = conversation.observe(from).await?;
    let mut view = View::default();
    let mut output_rows = Vec::new();
    let mut input = PromptInput::default();
    let mut viewport = Viewport::default();
    let start = Instant::now();
    loop {
        let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let size: Size = size()?;
        let busy = view.busy();
        input.busy = busy;
        let back = view.back;
        let (areas, page) = {
            let status = Line::raw(view.status());
            let screen = Screen {
                header: Line::styled("graphcoder", Style::default().add_modifier(Modifier::DIM)),
                transcript: view.rows(size.width.saturating_sub(1)),
                terminal: if busy { &output_rows[..] } else { &[] },
                status,
                ..Screen::default()
            };
            let area = Rect::from((Position::ORIGIN, size));
            let scroll = screen.tail_scroll(area, &input).saturating_sub(back);
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
                view.apply(&conversation, event).await?;
                // A settled turn clears the output with its display.
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
                    for effect in effects {
                        match effect {
                            InputEffect::Submit { text, .. } => {
                                if let Some(id) = view.approval().cloned() {
                                    let approved = matches!(text.trim(), "y" | "Y" | "yes");
                                    conversation.decide(&id, approved).await?;
                                    continue;
                                }
                                if view.pending.len() >= HISTORY as usize
                                    || text.len().saturating_add(view.pending.iter().map(|(_, text)| text.len()).sum::<usize>()) > 1024 * 1024
                                {
                                    view.failure = Some("pending prompt display is full; wait for a turn to settle".into());
                                    input.editor = Editor::new(&text);
                                    continue;
                                }
                                let id = acyclic_harness::OperationId::new().to_string();
                                let inbox = conversation.send(&id, &text).await?;
                                view.pending.push((inbox, text));
                                view.back = 0;
                                view.pending_dirty = true;
                            }
                            InputEffect::Interrupt => conversation.cancel().await?,
                            InputEffect::Quit => return Ok(0),
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
            }
        }
    }
}
