use acyclic_harness::{
    OperationId, client,
    local::LocalHarness,
    tui::{
        self, PromptBox, PromptInput, Viewport, crossterm,
        ratatui::layout::{Position, Rect, Size},
        term::{Environment, FrameTerminal, RegionScroll, ScreenMode, Tty},
    },
};
use std::{sync::Arc, time::Instant};

pub async fn run(
    local: Arc<LocalHarness>,
    operation: OperationId,
    inline: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let authority = local.client_owner();
    let own_task = acyclic_harness::TaskId::parse(&authority.id)?;
    let files = local.task_files();
    let mut client =
        client::TaskFileClient::observe_task(files.as_ref(), &authority, own_task, 1024, 4 << 20)
            .await?;
    let reader = local.client_reader()?;
    let mut commands = client::local_ui(&local, &operation.to_string(), &authority).await?;
    let (tty, mut terminal) = open_terminal(inline)?;
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
    loop {
        let task = commands.active_task().unwrap_or(own_task);
        client.follow_task(files.as_ref(), task).await?;
        renderer.update_files(&client, &authority);
        if commands.exit_requested()? {
            return Ok(());
        }
        prompt_files.refresh(&commands, &mut input)?;
        input.refresh_completions();
        let mut areas = tui::ScreenAreas::default();
        let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let back = client::tui_scroll(&commands)?;
        renderer.configure(client.views(), &authority, &commands)?;
        input.busy = renderer.is_busy();
        let size = terminal_size()?;
        renderer.layout(&commands, &input, size.width, size.height)?;
        let screen = renderer.screen_for(&commands)?;
        let area = Rect::from((Position::ORIGIN, size));
        let maximum_scroll = screen.tail_scroll(area, &input);
        if client::frame_due(&batch) {
            let scroll = maximum_scroll.saturating_sub(back);
            let transcript = screen.areas(area, &input).transcript;
            present(
                &mut terminal,
                size,
                &mut shown,
                !inline,
                (transcript, scroll),
                |frame| {
                    areas = screen.render(frame, &input, &mut viewport, scroll, now);
                },
            )?;
        } else {
            // More of this read is queued: route it against the same layout
            // and present once, after the read.
            areas = screen.areas(area, &input);
        }
        if batch.is_empty() {
            let Some(read) = client::next_file_tui_event(
                &mut client,
                &mut events,
                &mut completions,
                &mut input,
                &mut commands,
                areas.animation,
                &mut renderer,
                &mut documents,
            )
            .await?
            else {
                continue;
            };
            batch = read;
        }
        let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let area = PromptBox::input_area(areas.prompt);
        let page_rows = usize::from(areas.transcript.height);
        // A flick or held PageUp is one input batch: one scroll, one frame.
        client::input_burst(&mut batch, page_rows, &mut effects, |event| {
            input.event(event, area, &mut viewport, now)
        });
        client::file_tui_inputs(
            &mut commands,
            reader.as_ref(),
            effects.drain(..),
            page_rows,
            maximum_scroll,
        )
        .await?;
        prompt_files.save(&mut commands, &mut input).await?;
    }
}

fn terminal_size() -> std::io::Result<Size> {
    crossterm::terminal::size().map(|(width, height)| Size::new(width, height))
}

/// Opens the controlling terminal in the session's screen mode: its input
/// side and the frame terminal that presents to its output.
fn open_terminal(
    inline: bool,
) -> Result<(Tty, FrameTerminal<tui::term::Output>), Box<dyn std::error::Error>> {
    let mut tty = Tty::open()?;
    // Synchronized output follows the probed modes in the session journal.
    tty.negotiate(&Environment::capture())?;
    tty.setup(if inline {
        ScreenMode::Inline
    } else {
        ScreenMode::Alternate
    })?;
    let (tty, output) = tty.split();
    let terminal = FrameTerminal::new(output, terminal_size()?)?;
    Ok((tty, terminal))
}

/// The presentation boundary shared by the executable and its file-backed Rig.
/// Only successful device output advances the remembered transcript position.
pub(super) fn present<P: tui::term::Present>(
    terminal: &mut FrameTerminal<P>,
    size: Size,
    shown: &mut Option<(Rect, usize)>,
    regions: bool,
    (band, top): (Rect, usize),
    render: impl FnOnce(&mut tui::ratatui::Frame<'_>),
) -> std::io::Result<()> {
    let moved = RegionScroll::between(shown.filter(|_| regions), band, top);
    terminal.draw_scrolled(size, moved, render)?;
    *shown = Some((band, top));
    Ok(())
}
