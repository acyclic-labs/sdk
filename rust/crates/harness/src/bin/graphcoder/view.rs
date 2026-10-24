use acyclic_harness::{
    OperationId, client,
    local::LocalHarness,
    tui::{
        self, PromptInput, Viewport,
        ratatui::layout::{Position, Rect, Size},
        term::{Environment, FrameTerminal, RegionScroll, ScreenMode, Tty},
    },
};
use futures::StreamExt as _;
use std::{sync::Arc, time::Instant};
#[path = "view/diff.rs"]
mod diff;
use diff::DiffState;

pub async fn run(
    local: Arc<LocalHarness>,
    operation: OperationId,
    inline: bool,
) -> Result<client::HeadlessOutcome, Box<dyn std::error::Error>> {
    let authority = local.client_owner();
    let mut client = observe(&local, &authority).await?;
    let reader = local.client_reader()?;
    let mut commands = client::local_ui(&local, &operation.to_string(), &authority).await?;
    let tty = open_tty(inline)?;
    let terminal_size = tty.size_reader();
    let (tty, output) = tty.split();
    let mut terminal = FrameTerminal::new(output, terminal_size()?)?;
    let mut events = client::terminal_events(tty);
    // Events of the current read not yet handled; the read presents one frame.
    let mut batch = client::InputBatch::new();
    let mut effects = Vec::new();
    let mut input = PromptInput::default();
    let mut prompt_files = client::PromptFiles::default();
    prompt_files.refresh(&commands, &mut input)?;
    let mut completions = client::local_completions(&local, &mut input).await?;
    let mut viewport = Viewport::default();
    let mut renderer = tui::ClientRenderer::default();
    // The transcript band and its top line in the frame on the device.
    let mut shown = None;
    let start = Instant::now();
    client.follow_commands(&commands).await?;
    let mut documents = client::local_documents(&local, client.views_mut()).await?;
    let mut diff = DiffState::new();
    loop {
        if client::frame_due(&batch) {
            renderer.wait_frame().await;
        }
        follow_selected_task(local.as_ref(), &mut client, &commands, &authority).await?;
        renderer.update_files(&client, &authority);
        let diff_open = diff_is_open(&commands)?;
        refresh_diff(
            &mut diff,
            client.presentation_revision(),
            diff_open,
            &mut renderer,
            local.project_selection(),
            || follow_diff(local.clone()),
        )
        .await;
        if commands.exit_requested()? {
            return Ok(commands.exit_outcome()?);
        }
        prompt_files.refresh(&commands, &mut input)?;
        input.refresh_completions();
        let (areas, maximum_scroll) = render_view(
            &mut renderer,
            (&commands, &client, &authority),
            (&mut input, &mut viewport),
            (&mut terminal, terminal_size()?, &mut shown),
            (&batch, inline, &start),
        )?;
        if batch.is_empty() {
            let next = client::next_file_tui_event(
                &mut client,
                &mut events,
                &mut completions,
                &mut input,
                &mut commands,
                areas.animation,
                &mut renderer,
                &mut documents,
            );
            let Some(read) = (tokio::select! {
                read = next => read?,
                rows = async { match diff.stream() { Some(stream) => stream.next().await, None => std::future::pending().await } } => {
                    match rows {
                        Some(Ok(rows)) => renderer.update_diff(Ok(rows)),
                        failed => {
                            renderer.update_diff(failed.unwrap_or_else(|| {
                                Err(acyclic_harness::Error::Conflict("project diff follow ended".into()))
                            }));
                            diff.failed();
                        }
                    }
                    continue;
                }
            }) else {
                continue;
            };
            batch = read;
        }
        let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let page_rows = usize::from(areas.transcript.height);
        // A flick or held PageUp is one input batch: one scroll, one frame.
        client::input_burst(&mut batch, page_rows, &mut effects, |event| {
            areas.event(&mut input, event, &mut viewport, now, &mut |byte| {
                renderer.anchor_at(byte)
            })
        });
        client::file_tui_inputs(
            &mut commands,
            reader.as_ref(),
            effects.iter().cloned(),
            page_rows,
            maximum_scroll,
            |from, rows| renderer.move_view(from, rows),
        )
        .await?;
        prompt_files.save(&mut commands, &mut input).await?;
    }
}

fn open_tty(inline: bool) -> std::io::Result<Tty> {
    let mut tty = Tty::open()?;
    // Synchronized output follows the probed modes in the session journal.
    tty.negotiate(&Environment::capture())?;
    tty.setup(if inline {
        ScreenMode::Inline
    } else {
        ScreenMode::Alternate
    })?;
    Ok(tty)
}

async fn observe(
    local: &Arc<LocalHarness>,
    authority: &acyclic_harness::wire::Authority,
) -> acyclic_harness::Result<client::TaskFileClient> {
    client::TaskFileClient::observe_task(
        local.task_files().as_ref(),
        authority,
        acyclic_harness::TaskId::parse(&authority.id)?,
        1024,
        4 * 1024 * 1024,
    )
    .await
}

async fn refresh_diff<W: PartialEq, S>(
    diff: &mut DiffState<W, S>,
    revision: u64,
    open: bool,
    renderer: &mut tui::ClientRenderer,
    selection: impl std::future::Future<Output = acyclic_harness::Result<W>>,
    follow: impl FnOnce() -> S,
) {
    if open {
        if diff.needs_refresh(revision) {
            match selection.await {
                Ok(selected) => {
                    if diff.select(revision, selected, follow) {
                        renderer.update_diff(Ok(Vec::new()));
                    }
                }
                Err(error) => {
                    renderer.update_diff(Err(error));
                    diff.selection_failed(revision);
                }
            }
        }
    } else {
        diff.close();
    }
}

fn follow_diff(
    local: Arc<LocalHarness>,
) -> acyclic_stream::BoxProviderStream<
    'static,
    acyclic_harness::Result<Vec<tui::ratatui::text::Line<'static>>>,
> {
    client::local_diff(
        local,
        client::DiffLimits {
            files: 4096,
            changes: 4096,
            file_bytes: 65_536,
            render_bytes: 4 * 1024 * 1024,
            work: 16 * 1024 * 1024,
        },
    )
}


/// The presentation boundary shared by the executable and its file-backed Rig.
/// Only successful device output advances the remembered transcript position.
pub(super) fn present<P: tui::term::Present>(
    terminal: &mut FrameTerminal<P>,
    size: Size,
    shown: &mut Option<(Rect, tui::TranscriptAnchor)>,
    regions: bool,
    (band, top, moved): (Rect, Option<tui::TranscriptAnchor>, Option<isize>),
    render: impl FnOnce(&mut tui::ratatui::Frame<'_>),
) -> std::io::Result<()> {
    let moved = moved
        .filter(|rows| *rows != 0 && regions && shown.is_some_and(|(before, _)| before == band))
        .map(|rows| RegionScroll { region: band, rows });
    terminal.draw_scrolled(size, moved, render)?;
    *shown = top.map(|top| (band, top));
    Ok(())
}

fn render_view<P: tui::term::Present, S: acyclic_stream::StreamProvider + ?Sized>(
    renderer: &mut tui::ClientRenderer,
    files: (
        &client::CommandSession<S>,
        &client::TaskFileClient,
        &acyclic_harness::wire::Authority,
    ),
    prompt: (&mut PromptInput, &mut Viewport),
    device: (
        &mut FrameTerminal<P>,
        Size,
        &mut Option<(Rect, tui::TranscriptAnchor)>,
    ),
    frame: (&client::InputBatch, bool, &Instant),
) -> Result<(tui::ScreenAreas, usize), Box<dyn std::error::Error>> {
    let (commands, client, authority) = files;
    let (input, viewport) = prompt;
    let (terminal, size, shown) = device;
    let (batch, inline, start) = frame;
    let mut areas = tui::ScreenAreas::default();
    let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    let back = client::tui_modal_scroll(commands)?;
    renderer.configure(client.views(), authority, commands)?;
    input.busy = renderer.is_busy();
    renderer.layout(commands, input, size.width, size.height)?;
    let (source_top, moved) = renderer.presented_position(shown.map(|(_, top)| top));
    let screen = renderer.screen_for(commands)?;
    let area = Rect::from((Position::ORIGIN, size));
    let maximum_scroll = screen.tail_scroll(area, input);
    if client::frame_due(batch) {
        let scroll = maximum_scroll.saturating_sub(back);
        let layout = screen.areas(area, input);
        let mut transcript = layout.transcript;
        transcript.width += layout.scrollbar.width;
        present(
            terminal,
            size,
            shown,
            !inline,
            (transcript, source_top, moved),
            |frame| {
                areas = screen.render(frame, input, viewport, scroll, now);
            },
        )?;
        renderer.frame_presented();
    } else {
        // More of this read is queued: route it against the same layout
        // and present once, after the read.
        areas = screen.areas(area, input);
    }
    Ok((areas, maximum_scroll))
}

async fn follow_selected_task<S: acyclic_stream::StreamProvider + ?Sized>(
    local: &LocalHarness,
    client: &mut client::TaskFileClient,
    commands: &client::CommandSession<S>,
    authority: &acyclic_harness::wire::Authority,
) -> acyclic_harness::Result<()> {
    client
        .follow_task(
            local.task_files().as_ref(),
            commands
                .presentation_task()?
                .unwrap_or(acyclic_harness::TaskId::parse(&authority.id)?),
        )
        .await
}

fn diff_is_open<S: acyclic_stream::StreamProvider + ?Sized>(
    commands: &client::CommandSession<S>,
) -> acyclic_harness::Result<bool> {
    Ok(commands
        .state_ref(client::CommandFileRoot::Session, "layout.json")?
        .is_some_and(|layout| {
            layout.get("open_view").and_then(|value| value.as_str()) == Some("diff")
        }))
}
