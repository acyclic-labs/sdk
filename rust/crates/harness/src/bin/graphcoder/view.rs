use acyclic_harness::{
    OperationId, client,
    local::LocalHarness,
    tui::{
        self, PromptBox, PromptInput, Viewport,
        ratatui::{Terminal, backend::CrosstermBackend},
        term::{Environment, ScreenMode, Tty},
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
    tty.negotiate(&Environment::capture())?;
    tty.setup(if inline {
        ScreenMode::Inline
    } else {
        ScreenMode::Alternate
    })?;
    let (tty, output) = tty.split();
    let mut terminal = Terminal::new(CrosstermBackend::new(output))?;
    let mut events = client::terminal_events(tty);
    let mut input = PromptInput::default();
    let mut prompt_files = client::PromptFiles::default();
    prompt_files.refresh(&commands, &mut input)?;
    let mut completions = client::local_completions(&local, &mut input).await?;
    let mut viewport = Viewport::default();
    let mut renderer = tui::ClientRenderer::default();
    let start = Instant::now();
    let mut documents = client::local_documents(&local, &authority).await?;
    loop {
        client
            .follow_task(
                local.task_files().as_ref(),
                &authority,
                commands
                    .active_task()
                    .unwrap_or(acyclic_harness::TaskId::parse(&authority.id)?),
            )
            .await?;
        client.project_commands(&commands, &authority)?;
        renderer.update(client.views(), &authority);
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
        let size = terminal.size()?;
        renderer.layout(&commands, &input, size.width, size.height)?;
        let screen = renderer.screen_for(&commands)?;
        terminal.draw(|frame| {
            maximum_scroll = screen.tail_scroll(frame.area(), &input);
            let scroll = maximum_scroll.saturating_sub(back);
            areas = screen.render(frame, &input, &mut viewport, scroll, now);
        })?;
        let Some(event) = client::next_file_tui_event(
            &mut client,
            &mut events,
            &mut completions,
            &mut input,
            &mut commands,
            &mut documents,
        )
        .await?
        else {
            continue;
        };
        let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let effects = input.event(
            event,
            PromptBox::input_area(areas.prompt),
            &mut viewport,
            now,
        );
        for effect in effects {
            client::file_tui_input(
                &mut commands,
                reader.as_ref(),
                effect,
                usize::from(areas.transcript.height),
                maximum_scroll,
            )
            .await?;
        }
        prompt_files.save(&mut commands, &mut input).await?;
    }
}
