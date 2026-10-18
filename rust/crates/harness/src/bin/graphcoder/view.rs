use acyclic_harness::{
    OperationId, client,
    local::LocalHarness,
    tui::{
        self, PromptBox, PromptInput, Viewport, crossterm,
        ratatui::layout::Size,
        term::{Environment, FrameTerminal, ScreenMode, Tty},
    },
};
use std::{sync::Arc, time::Instant};

pub async fn run(
    local: Arc<LocalHarness>,
    operation: OperationId,
    inline: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let authority = local.client_owner();
    let mut client = client::TaskFileClient::observe_task(
        local.task_files().as_ref(),
        &authority,
        acyclic_harness::TaskId::parse(&authority.id)?,
        1024,
        4 * 1024 * 1024,
    )
    .await?;
    let reader = local.client_reader()?;
    let mut commands = client::local_ui(&local, &operation.to_string(), &authority).await?;
    let mut tty = Tty::open()?;
    // Synchronized output follows the probed modes in the session journal.
    tty.negotiate(&Environment::capture())?;
    tty.setup(if inline {
        ScreenMode::Inline
    } else {
        ScreenMode::Alternate
    })?;
    let (tty, output) = tty.split();
    let terminal_size =
        || crossterm::terminal::size().map(|(width, height)| Size::new(width, height));
    let mut terminal = FrameTerminal::new(output, terminal_size()?)?;
    let mut events = client::terminal_events(tty);
    // Events of the current read not yet handled; each gets its own frame.
    let mut batch = client::InputBatch::new();
    let mut effects = Vec::new();
    let mut input = PromptInput::default();
    let mut prompt_files = client::PromptFiles::default();
    prompt_files.refresh(&commands, &mut input)?;
    let mut completions = client::local_completions(&local, &mut input).await?;
    let mut viewport = Viewport::default();
    let mut renderer = tui::ClientRenderer::default();
    let start = Instant::now();
    client.follow_commands(&commands).await?;
    let mut documents = client::local_documents(&local, client.views_mut()).await?;
    loop {
        client
            .follow_task(
                local.task_files().as_ref(),
                commands
                    .active_task()
                    .unwrap_or(acyclic_harness::TaskId::parse(&authority.id)?),
            )
            .await?;
        renderer.update_files(&client, &authority);
        if commands.exit_requested()? {
            return Ok(());
        }
        prompt_files.refresh(&commands, &mut input)?;
        input.refresh_completions();
        let mut areas = tui::ScreenAreas::default();
        let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let back = client::tui_scroll(&commands)?;
        let mut maximum_scroll = 0;
        renderer.configure(client.views(), &authority, &commands)?;
        input.busy = renderer.is_busy();
        let size = terminal_size()?;
        renderer.layout(&commands, &input, size.width, size.height)?;
        let screen = renderer.screen_for(&commands)?;
        terminal.draw(size, |frame| {
            maximum_scroll = screen.tail_scroll(frame.area(), &input);
            let scroll = maximum_scroll.saturating_sub(back);
            areas = screen.render(frame, &input, &mut viewport, scroll, now);
        })?;
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
