//! The interactive terminal: one conversation's settled turns, its pending
//! prompts and its phase, over the shared prompt editor and screen widgets.
use acyclic_harness::local::{Event, agent_conversation::Conversation};
use acyclic_harness::{
    agent::{IntentId, Phase},
    tui::{
        InputEffect, PromptInput, Screen, Viewport,
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

/// One transcript block: a prompt or an answer.
struct Block {
    prompt: bool,
    text: String,
}

#[derive(Default)]
struct View {
    blocks: Vec<Block>,
    /// Sent prompts not yet settled, by inbox sequence.
    pending: Vec<(u64, String)>,
    phase: Option<Phase>,
    /// The latest failed delivery, until the next status.
    failure: Option<String>,
    /// Rows scrolled back from the live tail.
    back: usize,
    lines: Vec<Line<'static>>,
    width: u16,
    dirty: bool,
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
                    self.blocks.push(Block { prompt: true, text });
                }
                self.blocks.push(Block {
                    prompt: false,
                    text: answer,
                });
            }
            Event::Failed(error) => self.failure = Some(error),
            Event::Output => return Ok(()),
            Event::Status(status) => {
                self.failure = None;
                if matches!(status.phase, Phase::Idle | Phase::Done { .. }) {
                    self.pending.retain(|(inbox, _)| *inbox >= status.cursor);
                }
                self.phase = Some(status.phase);
            }
        }
        self.dirty = true;
        Ok(())
    }

    /// Wrapped transcript rows for `width` columns, rebuilt only on change.
    fn rows(&mut self, width: u16) -> &[Line<'static>] {
        if self.dirty || self.width != width {
            self.width = width;
            let width = usize::from(width.max(8)) - 2;
            self.lines.clear();
            let pending = self.pending.iter().map(|(_, text)| (true, text.as_str()));
            let blocks = self
                .blocks
                .iter()
                .map(|block| (block.prompt, block.text.as_str()));
            for (prompt, text) in blocks.chain(pending) {
                let style = if prompt {
                    Style::default().add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                let mut first = true;
                for line in text.lines() {
                    let characters = line.chars().collect::<Vec<_>>();
                    for chunk in characters
                        .chunks(width)
                        .chain(characters.is_empty().then_some(&[][..]))
                    {
                        let marker = match (prompt, first) {
                            (true, true) => "› ",
                            _ => "  ",
                        };
                        first = false;
                        let row = format!("{marker}{}", chunk.iter().collect::<String>());
                        self.lines.push(Line::styled(row, style));
                    }
                }
                self.lines.push(Line::default());
            }
            self.dirty = false;
        }
        &self.lines
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
            _ => "Enter sends · Ctrl+D quits".into(),
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
                let lines = events.output().lines().collect::<Vec<_>>();
                output_rows = lines[lines.len().saturating_sub(OUTPUT_ROWS)..]
                    .iter()
                    .map(|line| Line::raw((*line).to_owned()))
                    .collect();
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
                                let id = acyclic_harness::OperationId::new().to_string();
                                let inbox = conversation.send(&id, &text).await?;
                                view.pending.push((inbox, text));
                                view.back = 0;
                                view.dirty = true;
                            }
                            InputEffect::Interrupt => conversation.cancel().await?,
                            InputEffect::Quit => return Ok(0),
                            InputEffect::Scroll(rows) => {
                                view.back = view
                                    .back
                                    .saturating_add_signed(rows)
                                    .min(view.lines.len().saturating_sub(page));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}
