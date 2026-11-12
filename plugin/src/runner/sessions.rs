//! Selection is a client operation over the root directory and authored files.
use super::{Args, Command, Connection, OperationId};
use acyclic_harness::{
    agent::{AgentOutcome, Phase},
    client::AgentPageRequest,
    local::sessions,
    tui::{onboarding::Input, term::Tty},
};
type Failure = Box<dyn std::error::Error>;
mod picker;

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::{
        LocalHarness,
        agent::{AgentRecord as _, PeerMembership},
        local::WorkspaceMounting,
        model::Model,
        test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
    };
    use acyclic_stream::StreamProviderExt as _;
    use clap::Parser as _;
    use std::sync::Arc;

    #[tokio::test]
    async fn launch_replay_preserves_peer_choice_and_checks_owner_execution_policy()
    -> Result<(), Failure> {
        let directory = tempfile::tempdir()?;
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace)?;
        let provider = Arc::new(ScriptedModelProvider::new(
            std::iter::empty::<ScriptedTurn>(),
        ));
        let local = Arc::new(
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
                .workspace_mounting(WorkspaceMounting::Physical),
            )
            .await?,
        );
        let owner = local.connect_sessions().await?;
        let args = Args::try_parse_from([
            "graphcoder",
            "--model",
            "scripted/test",
            "--no-peer",
            "--questions",
            "true",
            "--operation",
            "launch",
            "exec",
            "task",
        ])?;
        let (first, _, created) = select(&args, owner.clone(), None).await?;
        apply_peer_choice(&args, &first, created).await?;
        sessions::set_peers(&first, "enable-later", true).await?;
        let (replayed, _, created) = select(&args, owner.clone(), None).await?;
        apply_peer_choice(&args, &replayed, created).await?;
        assert_eq!(first.root(), replayed.root());
        let (host, _) = local.agents().await?;
        assert!(
            host.state(first.root())
                .await?
                .profile
                .ok_or("missing creation profile")?
                .question_plugin
        );
        let head = host
            .provider()
            .head(replayed.root().peer_membership()?)
            .await?
            .last
            .ok_or("missing membership")?;
        assert!(PeerMembership::decode(&head.value)?.enabled);
        assert_eq!(head.sequence, 1, "replay must not append a new opt-out");
        let picker_args = Args::try_parse_from(["graphcoder", "--no-peer", "--resume"])?;
        let (picked, created) = choose_entry(&owner, None, !picker_args.no_peer, false).await?;
        assert!(created);
        let initial = host
            .provider()
            .head(picked.root().peer_membership()?)
            .await?
            .last
            .ok_or("missing picker membership")?;
        assert!(!PeerMembership::decode(&initial.value)?.enabled);
        assert_eq!(
            initial.sequence, 0,
            "picker must commit opt-out with creation"
        );
        sessions::set_peers(&picked, "picker-enable-later", true).await?;
        apply_peer_choice(&picker_args, &picked, created).await?;
        let current = host
            .provider()
            .head(picked.root().peer_membership()?)
            .await?
            .last
            .ok_or("missing picker membership")?;
        assert!(PeerMembership::decode(&current.value)?.enabled);
        assert_eq!(
            current.sequence, 1,
            "creation must not issue a later opt-out"
        );
        let (_, created) = choose_entry(&owner, Some(picked.root().clone()), true, false).await?;
        assert!(!created, "an existing picker row remains an attachment");
        let before =
            sessions::page(&owner, acyclic_harness::client::AgentPageRequest::default()).await?;
        let mismatch = Args::try_parse_from([
            "graphcoder",
            "--model",
            "scripted/other",
            "--operation",
            "other-model",
            "exec",
            "task",
        ])?;
        assert!(select(&mismatch, owner.clone(), None).await.is_err());
        let no_compaction = Args::try_parse_from([
            "graphcoder",
            "--no-compaction",
            "--operation",
            "unchecked",
            "exec",
            "task",
        ])?;
        assert!(select(&no_compaction, owner.clone(), None).await.is_err());
        let after =
            sessions::page(&owner, acyclic_harness::client::AgentPageRequest::default()).await?;
        assert_eq!(
            before.items.len(),
            after.items.len(),
            "execution policy refusal precedes root creation"
        );
        assert_eq!(provider.request_count(), 0);

        // This provider offers no context-capacity metadata. Only an explicitly
        // unchecked owner can run it without calibration or compaction admission.
        let unchecked_provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("unchecked answer".into())])
                .without_accounting(),
        );
        let unchecked = Arc::new(
            LocalHarness::open(
                local_harness_config(
                    &workspace,
                    (
                        Model::new("scripted".into(), "test".into(), "1".into())?,
                        unchecked_provider.clone(),
                    ),
                )?
                .without_user_settings()
                .state_directory(directory.path().join("unchecked-state"))
                .workspace_mounting(WorkspaceMounting::Physical)
                .compaction(acyclic_harness::context::CompactionPolicy::Disabled),
            )
            .await?,
        );
        let (selected, _, _) =
            select(&no_compaction, unchecked.connect_sessions().await?, None).await?;
        selected
            .require_execution(Some("scripted/test"), true)
            .await?;
        let conversation = acyclic_harness::local::agent_conversation::Conversation::open(selected);
        let mut events = conversation.observe(None).await?;
        let inbox = conversation.send("unchecked-prompt", "task").await?;
        let settled = conversation
            .settle("unchecked-prompt", inbox, &mut events)
            .await?;
        let acyclic_harness::local::agent_conversation::Settlement::Reply(
            acyclic_harness::agent::Reply::Answered {
                sequence, entry, ..
            },
        ) = settled
        else {
            return Err("unchecked prompt did not answer".into());
        };
        assert_eq!(
            conversation.output_text_at(sequence, &entry).await?,
            "unchecked answer"
        );
        assert_eq!(unchecked_provider.request_count(), 1);
        Ok(())
    }
}

/// Creation already committed the launch choice, including on an exact replay.
/// Only attaching to an existing session issues a new user membership command.
pub async fn apply_peer_choice(
    args: &Args,
    connection: &Connection,
    created: bool,
) -> Result<(), Failure> {
    if args.no_peer && !created {
        sessions::set_peers(
            connection,
            &format!("peers-off:{}", OperationId::new()),
            false,
        )
        .await?;
    }
    Ok(())
}

pub async fn select(
    args: &Args,
    connection: Connection,
    terminal: Option<Tty>,
) -> Result<(Connection, Option<Tty>, bool), Failure> {
    let original = connection.root().clone();
    let (selected, terminal, created) = select_inner(args, connection, terminal).await?;
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
    Ok((selected, terminal, created))
}

async fn select_inner(
    args: &Args,
    connection: Connection,
    terminal: Option<Tty>,
) -> Result<(Connection, Option<Tty>, bool), Failure> {
    if args.agent.is_some() || args.fork_from.is_some() {
        return Ok((connection, terminal, false));
    }
    if let Some(resume) = &args.resume {
        if let Some(name) = resume {
            return Ok((sessions::select(&connection, name).await?, terminal, false));
        }
        connection
            .require_execution(args.model.as_deref(), args.no_compaction)
            .await?;
        let prices = args
            .prices
            .as_deref()
            .map(acyclic_harness::local::read_regular_file)
            .transpose()?
            .map(String::from_utf8)
            .transpose()?;
        return choose(
            connection,
            terminal,
            None,
            prices,
            super::questions_enabled(args),
            !args.no_peer,
        )
        .await;
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
        return Ok((selected, terminal, false));
    }
    let local_execution = args.endpoint.is_none()
        && matches!(
            args.command,
            None | Some(Command::Run | Command::Exec { .. })
        );
    if args.new_session
        || local_execution
        || matches!(args.command, Some(Command::NewSession { .. }))
    {
        connection
            .require_execution(args.model.as_deref(), args.no_compaction)
            .await?;
        let operation = args
            .operation
            .clone()
            .unwrap_or_else(|| OperationId::new().to_string());
        let selected = sessions::create_with_peer(
            &connection,
            &operation,
            !args.no_peer,
            Some(super::questions_enabled(args)),
        )
        .await?;
        if let Some(Command::NewSession { title: Some(title) }) = &args.command {
            sessions::rename(&selected, OperationId::new(), title.clone()).await?;
        }
        return Ok((selected, terminal, true));
    }
    Ok((connection, terminal, false))
}

/// The existing event-driven terminal chooser renders only the current page.
/// Advancing is an explicit user action; it never searches hidden pages.
pub async fn picker(
    connection: Connection,
    terminal: Option<Tty>,
    filter: Option<&str>,
    prices: Option<String>,
    questions: bool,
) -> Result<(Connection, Option<Tty>), Failure> {
    let (selected, terminal, _) =
        choose(connection, terminal, filter, prices, questions, true).await?;
    sessions::selected(&selected, &OperationId::new().to_string()).await?;
    Ok((selected, terminal))
}

async fn choose(
    connection: Connection,
    terminal: Option<Tty>,
    filter: Option<&str>,
    prices: Option<String>,
    questions: bool,
    peer: bool,
) -> Result<(Connection, Option<Tty>, bool), Failure> {
    let prices = prices
        .as_deref()
        .map(|text| acyclic_harness::executor::ModelPrices::from_jsonl(text.as_bytes()))
        .transpose()?
        .map(std::sync::Arc::new);
    let mut terminal = match terminal {
        Some(terminal) => terminal,
        None => Tty::open()?,
    };
    let mut cursor = None;
    let mut previous = Vec::new();
    loop {
        let height = terminal.size_reader()()?.height;
        let rows = u32::from(height.saturating_sub(12).max(1)).min(128);
        let page = sessions::page_filtered(
            &connection,
            AgentPageRequest::new(rows, 64 * 1024, cursor.clone())?,
            &sessions::Filter {
                title: filter.map(str::to_owned),
                ..Default::default()
            },
        )
        .await?;
        let visible: Vec<_> = page.items.into_iter().collect();
        let mut choices: Vec<_> = visible
            .iter()
            .map(|row| {
                let title: String = row
                    .title
                    .as_deref()
                    .filter(|title| !title.trim().is_empty())
                    .unwrap_or("Untitled session")
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
                    match row.status.as_ref().map(|status| &status.phase) {
                        None | Some(Phase::Idle) => "idle",
                        Some(Phase::Running) => "working",
                        Some(Phase::WaitingApproval { .. }) => "approval needed",
                        Some(Phase::WaitingUser { .. }) => "answer needed",
                        Some(Phase::WaitingHost { .. }) => "waiting for host",
                        Some(Phase::Waiting { input: true, .. }) => {
                            "waiting for input or work"
                        }
                        Some(Phase::Waiting { .. }) => "waiting for work",
                        Some(Phase::Done {
                            outcome: AgentOutcome::Archived,
                        }) => "archived",
                        Some(Phase::Done {
                            outcome: AgentOutcome::Succeeded { .. },
                        }) => "completed",
                        Some(Phase::Done { .. }) => "failed",
                    }
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
        let (owned, choice) = picker::choose(
            terminal,
            connection.clone(),
            visible.iter().map(|row| row.agent.clone()).collect(),
            choices,
            prices.clone(),
        )
        .await?;
        terminal = owned;
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
        let entry = if choice == new_index {
            None
        } else {
            Some(
                visible
                    .get(choice)
                    .ok_or("missing selected session")?
                    .agent
                    .clone(),
            )
        };
        let (selected, created) = choose_entry(&connection, entry, peer, questions).await?;
        return Ok((selected, Some(terminal), created));
    }
}

async fn choose_entry(
    connection: &Connection,
    entry: Option<acyclic_harness::agent::AgentPath>,
    peer: bool,
    questions: bool,
) -> Result<(Connection, bool), Failure> {
    match entry {
        Some(agent) => Ok((connection.select(agent).await?, false)),
        None => Ok((
            sessions::create_with_peer(
                connection,
                &OperationId::new().to_string(),
                peer,
                Some(questions),
            )
            .await?,
            true,
        )),
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
