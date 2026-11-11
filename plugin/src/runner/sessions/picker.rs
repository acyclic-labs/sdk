//! The visible page and one highlighted session's disposable live previews.
use super::*;
use acyclic_harness::{
    client::{DiffProjection, DiffQueryEvent, DiffSummary, InputPreviewStep},
    local::agent_conversation::Conversation,
    tui::{
        key_event::{InputEvent, KeyCode, KeyEventKind, KeyModifiers},
        ratatui::{
            style::{Modifier, Style},
            text::Line,
            widgets::Paragraph,
        },
        term::{Environment, FrameTerminal, ScreenMode, input_events},
    },
};
use futures::{StreamExt as _, stream::LocalBoxStream};
use std::sync::Arc;

type Update = (usize, String);

fn previews(
    connection: Connection,
    agent: acyclic_harness::agent::AgentPath,
    bytes: u32,
) -> LocalBoxStream<'static, Update> {
    async_stream::stream! {
        let selected = match connection.select(agent).await {
            Ok(selected) => Conversation::open(selected),
            Err(error) => {
                yield (0, format!("Preview unavailable: {error}"));
                return;
            }
        };
        let input = Arc::clone(&selected);
        let prompt: LocalBoxStream<'static, Update> = async_stream::stream! {
            let result = async {
                let mut query = input.input_query(AgentPageRequest::new(1, bytes, None)?).await?;
                while let Some(event) = query.next().await? {
                    match event {
                        InputPreviewStep::Progress => tokio::task::yield_now().await,
                        InputPreviewStep::Selected(value) => return Ok::<_, Failure>(value),
                    }
                }
                Err("input query ended without a selection".into())
            }.await;
            let label = match result {
                Ok(Some(preview)) => format!("Last prompt: {}{}", printable(&preview.text),
                    if (preview.text.len() as u64) < preview.bytes { "…" } else { "" }),
                Ok(None) => "Last prompt: none".into(),
                Err(error) => format!("Last prompt unavailable: {error}"),
            };
            yield (0, label);
        }.boxed_local();
        let changes = Arc::clone(&selected);
        let diff: LocalBoxStream<'static, Update> = async_stream::stream! {
            let mut summary = DiffSummary::default();
            let result = async {
                let mut query = changes.diff_query(AgentPageRequest::new(1, 2048, None)?, DiffProjection::Summary).await?;
                while let Some(event) = query.next().await? {
                    if let DiffQueryEvent::Summary(value) = event { summary = value; }
                    tokio::task::yield_now().await;
                }
                Ok::<_, Failure>(())
            }.await;
            yield (1, match result {
                Ok(()) => format!("Diff: {} files · +{} −{} · {} omitted", summary.files, summary.added, summary.removed, summary.omitted),
                Err(error) => format!("Diff unavailable: {error}"),
            });
        }.boxed_local();
        let cost: LocalBoxStream<'static, Update> = async_stream::stream! {
            // Read only the explicitly admitted UI-session price file. An
            // absent file leaves usage unpriced; no currency is guessed.
            let prices = async {
                let files = selected.client_files(selected.client_session(), vec!["prices.jsonl".into()], None).await?;
                let prices = files.files.get("prices.jsonl").and_then(Option::as_ref)
                    .map(|file| acyclic_harness::executor::ModelPrices::from_jsonl(file.text.as_bytes()))
                    .transpose()?.unwrap_or_default();
                Ok::<_, Failure>(Arc::new(prices))
            }.await;
            match prices {
                Err(error) => yield (2, format!("Cost unavailable: {error}")),
                Ok(prices) => {
                    let mut query = selected.usage_query_with_prices(prices);
                    loop {
                        if let Err(error) = query.advance().await {
                            yield (2, format!("Cost unavailable: {error}"));
                            break;
                        }
                        if query.synchronized() {
                            yield (2, format!("Cost: {}", acyclic_harness::tui::usage_summary(query.totals())));
                        }
                        tokio::task::yield_now().await;
                    }
                }
            }
        }.boxed_local();
        let mut streams = futures::stream::select_all(vec![prompt, diff, cost]);
        while let Some(update) = streams.next().await { yield update; }
    }.boxed_local()
}

fn printable(text: &str) -> String {
    text.chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect()
}

pub(super) async fn choose(
    mut tty: Tty,
    connection: Connection,
    agents: Vec<acyclic_harness::agent::AgentPath>,
    choices: Vec<String>,
) -> Result<(Tty, usize), Failure> {
    tty.negotiate(&Environment::capture())?;
    tty.setup(ScreenMode::Alternate)?;
    let size = tty.size_reader();
    let (tty, output) = tty.split();
    let mut terminal = FrameTerminal::new(output, size()?)?;
    let mut keys = input_events(tty)?;
    let mut selected = 0;
    let mut digits = String::new();
    let mut fields = [
        "Last prompt loading…".into(),
        "Diff loading…".into(),
        "Cost loading…".into(),
    ];
    let mut source: LocalBoxStream<'static, Update> = Box::pin(futures::stream::empty());
    let mut source_done = true;
    let mut changed = true;
    let choice = loop {
        if changed {
            fields = if agents.get(selected).is_some() {
                [
                    "Last prompt loading…".into(),
                    "Diff loading…".into(),
                    "Cost loading…".into(),
                ]
            } else {
                Default::default()
            };
            source = match agents.get(selected) {
                Some(agent) => previews(
                    connection.clone(),
                    agent.clone(),
                    u32::from(size()?.width).saturating_mul(4).saturating_add(9),
                ),
                None => Box::pin(futures::stream::empty()),
            };
            source_done = agents.get(selected).is_none();
            changed = false;
        }
        terminal.draw(size()?, |frame| {
            let mut lines = vec![Line::raw("Session")];
            lines.extend(choices.iter().enumerate().map(|(index, label)| {
                let row = Line::raw(format!("{}. {label}", index + 1));
                if index == selected {
                    row.style(Style::default().add_modifier(Modifier::REVERSED))
                } else {
                    row
                }
            }));
            lines.push(Line::raw(""));
            lines.extend(fields.iter().cloned().map(Line::raw));
            lines.push(Line::raw(format!(
                "Session: {digits} · ↑/↓ preview · number + Enter selects · Esc cancels"
            )));
            frame.render_widget(Paragraph::new(lines), frame.area());
        })?;
        tokio::select! {
            biased;
            batch = keys.next() => {
                let batch = batch.ok_or("terminal input ended")??;
                let mut chosen = None;
                for event in batch {
                    if let InputEvent::Key(key) = event {
                        if key.kind == KeyEventKind::Release { continue; }
                        match key.code {
                            KeyCode::Esc => return Err("session selection cancelled".into()),
                            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return Err("session selection cancelled".into()),
                            KeyCode::Up | KeyCode::Down => {
                                digits.clear();
                                let next = if key.code == KeyCode::Up { selected.saturating_sub(1) }
                                    else { (selected + 1).min(choices.len().saturating_sub(1)) };
                                changed |= selected != next;
                                selected = next;
                            }
                            KeyCode::Char(ch) if ch.is_ascii_digit() => {
                                digits.push(ch);
                                if let Some(next) = digits.parse::<usize>().ok().and_then(|value| value.checked_sub(1)).filter(|value| *value < choices.len()) {
                                    changed |= selected != next;
                                    selected = next;
                                }
                            }
                            KeyCode::Backspace => { digits.pop(); }
                            KeyCode::Enter if digits.is_empty() || digits.parse::<usize>().ok() == selected.checked_add(1) => { chosen = Some(selected); break; }
                            _ => {}
                        }
                    }
                }
                if let Some(chosen) = chosen { break chosen; }
            }
            update = source.next(), if !source_done => {
                if let Some((index, label)) = update { fields[index] = printable(&label); }
                else { source_done = true; }
            }
        }
    };
    drop(source);
    drop(keys);
    drop(terminal);
    Ok((Tty::open()?, choice))
}
