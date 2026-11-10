//! Selection is a client operation over the root directory and authored files.
use crate::{Args, Command, Connection, OperationId};
use acyclic_harness::{
    client::AgentPageRequest,
    local::sessions,
    tui::{onboarding::Input, term::Tty},
};
type Failure = Box<dyn std::error::Error>;

pub async fn select(
    args: &Args,
    connection: Connection,
    terminal: Option<Tty>,
) -> Result<(Connection, Option<Tty>), Failure> {
    let original = connection.root().clone();
    let (selected, terminal) = select_inner(args, connection, terminal).await?;
    if selected.root() != &original
        && args.agent.is_none()
        && selected.root().parent().is_none()
        && (args.resume.is_some()
            || args.continue_session
            || args.new_session
            || matches!(
                args.command,
                Some(
                    Command::NewSession { .. }
                        | Command::Name { .. }
                        | Command::Rename { .. }
                        | Command::Archive
                )
            )
            || args.command.is_none() && args.prompt.is_none())
    {
        sessions::selected(&selected, &OperationId::new().to_string()).await?;
    }
    Ok((selected, terminal))
}

async fn select_inner(
    args: &Args,
    connection: Connection,
    terminal: Option<Tty>,
) -> Result<(Connection, Option<Tty>), Failure> {
    if args.agent.is_some() || args.fork_from.is_some() {
        return Ok((connection, terminal));
    }
    if let Some(resume) = &args.resume {
        if let Some(name) = resume {
            return Ok((sessions::select(&connection, name).await?, terminal));
        }
        return choose(connection, terminal, None).await;
    }
    if args.continue_session
        || matches!(
            args.command,
            Some(Command::Name { .. } | Command::Rename { .. } | Command::Archive)
        )
    {
        let selected = sessions::latest(&connection)
            .await?
            .ok_or("No previous session for this checkout; start graphcoder or graphcoder new")?;
        return Ok((selected, terminal));
    }
    let interactive = args.command.is_none()
        && args.prompt.is_none()
        && args.input_format == crate::InputFormat::Text
        && args.output_format == crate::OutputFormat::Text
        && !acyclic_harness::tui::term::Environment::capture().is_dumb();
    if args.new_session || interactive || matches!(args.command, Some(Command::NewSession { .. })) {
        let operation = args
            .operation
            .clone()
            .unwrap_or_else(|| OperationId::new().to_string());
        let selected = sessions::create(&connection, &operation).await?;
        if let Some(Command::NewSession { title: Some(title) }) = &args.command {
            sessions::rename(&selected, OperationId::new(), title.clone()).await?;
        }
        return Ok((selected, terminal));
    }
    Ok((connection, terminal))
}

/// The existing event-driven terminal chooser renders only the current page.
/// Advancing is an explicit user action; it never searches hidden pages.
pub async fn picker(
    connection: Connection,
    terminal: Option<Tty>,
    filter: Option<&str>,
) -> Result<(Connection, Option<Tty>), Failure> {
    let (selected, terminal) = choose(connection, terminal, filter).await?;
    sessions::selected(&selected, &OperationId::new().to_string()).await?;
    Ok((selected, terminal))
}

async fn choose(
    connection: Connection,
    terminal: Option<Tty>,
    filter: Option<&str>,
) -> Result<(Connection, Option<Tty>), Failure> {
    let mut terminal = match terminal {
        Some(terminal) => terminal,
        None => Tty::open()?,
    };
    let mut cursor = None;
    let mut previous = Vec::new();
    loop {
        let height = terminal.size_reader()()?.height;
        let rows = u32::from(height.saturating_sub(9).max(1)).min(128);
        let page = sessions::page(
            &connection,
            AgentPageRequest::new(rows, 64 * 1024, cursor.clone())?,
        )
        .await?;
        let visible: Vec<_> = page
            .items
            .into_iter()
            .filter(|row| {
                filter.is_none_or(|filter| {
                    row.agent.as_str().contains(filter)
                        || row
                            .title
                            .as_ref()
                            .is_some_and(|title| title.contains(filter))
                })
            })
            .collect();
        let mut choices: Vec<_> = visible
            .iter()
            .map(|row| {
                let title: String = row
                    .title
                    .as_deref()
                    .unwrap_or("New session")
                    .chars()
                    .filter(|ch| !ch.is_control())
                    .take(usize::from(
                        terminal.size_reader()()
                            .map_or(80, |size| size.width)
                            .saturating_sub(20),
                    ))
                    .collect();
                format!(
                    "{title} · {} · {}",
                    row.agent.id(),
                    row.status
                        .as_ref()
                        .map_or("new".into(), |status| format!("{:?}", status.phase))
                )
            })
            .collect();
        let next_index = page.next.as_ref().map(|_| {
            let index = choices.len();
            choices.push("Next page".into());
            index
        });
        let previous_index = (!previous.is_empty()).then(|| {
            let index = choices.len();
            choices.push("Previous page".into());
            index
        });
        let new_index = choices.len();
        choices.push("New session".into());
        let rename_index = choices.len();
        choices.push("Rename a session".into());
        let archive_index = choices.len();
        choices.push("Archive a session".into());
        let row_choices = choices[..visible.len()].to_vec();
        let (owned, choice) = tokio::task::spawn_blocking(move || {
            let choice =
                Input::new(&mut terminal).and_then(|mut input| input.choose("Session", &choices));
            (terminal, choice)
        })
        .await?;
        terminal = owned;
        let choice = choice?;
        if Some(choice) == next_index {
            previous.push(cursor);
            cursor = page.next;
            continue;
        }
        if Some(choice) == previous_index {
            cursor = previous.pop().ok_or("missing previous session page")?;
            continue;
        }
        if choice == rename_index || choice == archive_index {
            if visible.is_empty() {
                continue;
            }
            let (owned, chosen) = tokio::task::spawn_blocking(move || {
                let chosen = Input::new(&mut terminal)
                    .and_then(|mut input| input.choose("Session", &row_choices));
                (terminal, chosen)
            })
            .await?;
            terminal = owned;
            let selected = connection
                .select(
                    visible
                        .get(chosen?)
                        .ok_or("missing selected session")?
                        .agent
                        .clone(),
                )
                .await?;
            if choice == archive_index {
                sessions::archive(&selected, &OperationId::new().to_string()).await?;
            } else {
                let (owned, title) = tokio::task::spawn_blocking(move || {
                    let title =
                        Input::new(&mut terminal).and_then(|mut input| input.read_text("Title"));
                    (terminal, title)
                })
                .await?;
                terminal = owned;
                sessions::rename(&selected, OperationId::new(), title?).await?;
            }
            continue;
        }
        let selected = if choice == new_index {
            sessions::create(&connection, &OperationId::new().to_string()).await?
        } else {
            connection
                .select(
                    visible
                        .get(choice)
                        .ok_or("missing selected session")?
                        .agent
                        .clone(),
                )
                .await?
        };
        return Ok((selected, Some(terminal)));
    }
}

/// Select one visible coupled cut. The source stays available when the new
/// paused root becomes current; paging never reads hidden history.
pub async fn history_picker(
    connection: Connection,
    terminal: Option<Tty>,
) -> Result<(Connection, Option<Tty>), Failure> {
    let mut terminal = match terminal {
        Some(terminal) => terminal,
        None => Tty::open()?,
    };
    let conversation =
        acyclic_harness::local::agent_conversation::Conversation::open(connection.clone());
    let mut cursor = None;
    let mut previous = Vec::new();
    loop {
        let rows = u32::from(terminal.size_reader()()?.height.saturating_sub(8).max(1)).min(128);
        let page = conversation
            .history_page(AgentPageRequest::new(rows, 64 * 1024, cursor.clone())?)
            .await?;
        let mut choices: Vec<_> = page
            .items
            .iter()
            .map(|cut| {
                format!(
                    "Turn {} · {} project volumes · context through {}",
                    cut.conversation, cut.volume_count, cut.context_tail,
                )
            })
            .collect();
        let older = page.next.as_ref().map(|_| {
            let index = choices.len();
            choices.push("Older turns".into());
            index
        });
        let newer = (!previous.is_empty()).then(|| {
            let index = choices.len();
            choices.push("Newer turns".into());
            index
        });
        let cancel = choices.len();
        choices.push("Keep current session".into());
        let (owned, chosen) = tokio::task::spawn_blocking(move || {
            let chosen = Input::new(&mut terminal).and_then(|mut input| {
                input.choose(
                    "Fork / rewind to turn (new session starts paused)",
                    &choices,
                )
            });
            (terminal, chosen)
        })
        .await?;
        terminal = owned;
        let chosen = chosen?;
        if Some(chosen) == older {
            previous.push(cursor);
            cursor = page.next;
            continue;
        }
        if Some(chosen) == newer {
            cursor = previous.pop().ok_or("missing newer history page")?;
            continue;
        }
        if chosen == cancel {
            return Ok((connection, Some(terminal)));
        }
        let cut = page
            .items
            .get(chosen)
            .ok_or("missing selected history cut")?;
        let selected = sessions::fork(
            &connection,
            connection.root().clone(),
            cut.conversation.checked_sub(1).ok_or("empty history cut")?,
            &OperationId::new().to_string(),
        )
        .await?;
        return Ok((selected, Some(terminal)));
    }
}
