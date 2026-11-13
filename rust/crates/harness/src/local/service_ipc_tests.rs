use super::*;
use crate::{
    agent::{Sender, TurnInput},
    local::{
        agent_conversation::{Conversation, Sent},
        sessions,
    },
    model::Model,
    test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
};
use std::sync::atomic::{AtomicBool, Ordering};

const DIRECTORY: &str = "ACYCLIC_IPC_TEST_DIRECTORY";
const SENDER: &str = "ACYCLIC_IPC_TEST_SENDER";
const RECIPIENT: &str = "ACYCLIC_IPC_TEST_RECIPIENT";
const TEST: &str = "local::service::ipc_tests::external_process_delivers_attributed_peer_input_to_a_running_session";

#[tokio::test]
async fn external_process_delivers_attributed_peer_input_to_a_running_session() -> Result<()> {
    if let Some(directory) = std::env::var_os(DIRECTORY) {
        let connection = Directory(PathBuf::from(directory)).connect().await?;
        let sender = crate::agent::AgentPath::try_from(std::env::var(SENDER).map_err(storage)?)
            .map_err(storage)?;
        let recipient =
            crate::agent::AgentPath::try_from(std::env::var(RECIPIENT).map_err(storage)?)
                .map_err(storage)?;
        let sender = Conversation::open(connection.select(sender).await?);
        let message = sender
            .prepare_peer_message(
                recipient,
                "external-message",
                "message from another process",
            )
            .await?;
        let Sent::Accepted(inbox) = sender.deliver_peer_message(&message).await? else {
            return Err(Error::Storage("peer message was not accepted".into()));
        };
        // Reusing the same prepared command returns its original inbox receipt.
        assert!(matches!(
            sender.deliver_peer_message(&message).await?,
            Sent::Accepted(replayed) if replayed == inbox
        ));
        return Ok(());
    }
    for owned_descendant in [false, true] {
        external_message_case(owned_descendant).await?;
    }
    Ok(())
}

async fn external_message_case(owned_descendant: bool) -> Result<()> {
    let workspace = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    let dispatch_enabled = Arc::new(AtomicBool::new(false));
    let enabled = dispatch_enabled.clone();
    let provider = Arc::new(
        ScriptedModelProvider::new(if owned_descendant {
            vec![ScriptedTurn::Text("ready".into())]
        } else {
            Vec::new()
        })
        .with_context_capacity(crate::context::ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: Some(4096),
            output_shares_context: false,
        })
        .with_dispatch_observer(move |_| {
            if enabled.load(Ordering::SeqCst)
                && let Some(entered) = entered.lock().unwrap().take()
            {
                let _ = entered.send(());
            }
            Box::pin(async { Ok(()) })
        }),
    );
    let config = local_harness_config(
        workspace.path(),
        (
            Model::new("scripted".into(), "test".into(), "1".into())?,
            provider.clone(),
        ),
    )?
    .state_directory(state.path())
    .workspace_mounting(crate::local::WorkspaceMounting::Physical)
    .without_default_tools();
    let directory = Directory::for_config(&config)?;
    let owner = directory.own()?;
    let listener = owner.listener()?;
    let local = Arc::new(super::super::LocalHarness::open(config).await?);
    let connection = local.connect_sessions().await?;
    let sender = sessions::create(&connection, "sender", None).await?;
    let recipient = if owned_descendant {
        let crate::client::AgentReply::Spawned { agent, .. } = sender
            .execute(&crate::client::AgentCommand::Spawn {
                parent: sender.root().clone(),
                id: crate::agent::IntentId::try_from("named-worker").map_err(storage)?,
                brief: crate::model::ModelContent::Text(
                    "Prepare to receive a supervision message.".into(),
                ),
                role: "worker".into(),
            })
            .await?
        else {
            return Err(Error::Storage("named child was not spawned".into()));
        };
        let selected = sender.select(agent).await?;
        Conversation::open(selected.clone())
            .wait_current_input()
            .await?;
        // Turning off default peering cannot revoke the owner's existing tree
        // controls or prevent its ordinary parent-to-descendant messages.
        sessions::set_peers(&sender, "disable-default-peering", false).await?;
        selected
    } else {
        sessions::create(&connection, "recipient", None).await?
    };
    let before = Conversation::open(recipient.clone())
        .wait_current_input()
        .await?
        .conversation;
    let before_requests = provider.request_count();
    provider.extend([
        ScriptedTurn::Text("received".into()),
        ScriptedTurn::Text("no follow-up work".into()),
    ]);
    dispatch_enabled.store(true, Ordering::SeqCst);
    let hosted = local
        .agents
        .get()
        .ok_or_else(|| Error::Invalid("missing owner".into()))?;
    let changes = hosted.server().bound().changes().map_err(storage)?;
    let config = crate::server::ServerConfig::new(crate::server::Hosted::new(
        Arc::clone(hosted.server()),
        local.clock.clone(),
        local.clone(),
        Some(Arc::new(changes)),
    ));
    let (ready, readiness) = tokio::sync::oneshot::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let result = crate::server::serve(
            listener,
            config,
            |endpoint| {
                let _ = ready.send(owner.publish(endpoint));
            },
            async {
                let _ = stopped.await;
            },
        )
        .await;
        drop(owner);
        result
    });
    let result = async {
        readiness.await.map_err(storage)??;
        let recipient = Conversation::open(recipient);
        let mut events = recipient.observe(None).await?;
        let mut command = acyclic_native_runtime::ProcessCommand::new(std::env::current_exe().map_err(storage)?);
        command.args(["--exact", TEST, "--nocapture"])
            .env(DIRECTORY, directory.0.as_os_str())
            .env(SENDER, sender.root().as_str())
            .env(RECIPIENT, recipient.agent().as_str())
            .stdout(acyclic_native_runtime::ProcessStdio::null())
            .stderr(acyclic_native_runtime::ProcessStdio::inherit());
        let mut child = acyclic_native_runtime::ProcessTree::spawn(&mut command).map_err(storage)?;
        child.readiness().map_err(storage)?.await.into_result().map_err(storage)?;
        assert!(child.try_wait(None).map_err(storage)?.ok_or_else(|| Error::Storage("missing child exit".into()))?.success());
        child.terminate(None).map_err(storage)?;
        let mut entering = std::pin::pin!(entering);
        loop {
            tokio::select! {
                biased;
                entered = &mut entering => { entered.map_err(storage)?; break; },
                event = events.next() => match event? {
                    super::super::Event::Turn { entry, .. } => return Err(Error::Storage(format!(
                        "peer turn ended before model dispatch: {:?}", entry.outcome,
                    ))),
                    super::super::Event::Status(status) if matches!(status.phase, crate::agent::Phase::Done { .. }) => {
                        return Err(Error::Storage(format!("recipient ended before model dispatch: {:?}", status.phase)));
                    }
                    _ => {},
                }
            }
        }
        let settled = recipient.wait_current_input().await?;
        assert_eq!(settled.conversation, before + 1);
        assert!(matches!(settled.phase, crate::agent::Phase::Idle));
        let entries = recipient.entries(before, before + 1).await?;
        assert!(matches!(entries.as_slice(), [entry] if matches!(&entry.input, TurnInput::Message { from: Sender::Agent { path }, .. } if path == sender.root())));
        assert_eq!(recipient.output_text_at(before, &entries[0]).await?, "received");
        assert!(provider.requests()[before_requests..].iter().any(|request|
            request.messages.iter().flat_map(|message| message.content.parts()).any(|part|
                matches!(part, crate::model::ModelContentPart::Notices { notices } if notices.iter().any(|notice|
                    notice.kind == "peer.message" && notice.source == sender.root().as_str() && notice.body["id"] == "external-message"))
            )), "external message was not rendered as peer input");
        let sender = Conversation::open(sender);
        if !owned_descendant { assert!(sender.text(&entries[0].output).await.is_err()); }

        Ok(())
    }.await;
    let _ = stop.send(());
    let served = server.await.map_err(storage)?;
    result.and(served)
}

#[tokio::test]
async fn live_observation_reconnects_through_authenticated_ipc() -> Result<()> {
    use futures::StreamExt as _;
    let workspace = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let config = local_harness_config(
        workspace.path(),
        (
            Model::new("scripted".into(), "test".into(), "1".into())?,
            Arc::new(ScriptedModelProvider::new([])),
        ),
    )?
    .state_directory(state.path())
    .without_default_tools();
    let directory = Directory::for_config(&config)?;
    let owner = directory.own()?;
    let crate::server::Listener::Local(incoming) = owner.listener()? else {
        return Err(Error::Invalid("local owner requires IPC".into()));
    };
    let (cut, cutting) = tokio::sync::oneshot::channel();
    let (reconnected, reconnecting) = tokio::sync::oneshot::channel();
    let mut cutting = Some(cutting);
    let mut reconnected = Some(reconnected);
    let incoming = incoming.map(move |connection| {
        connection.map(|mut connection| {
            if let Some(cutting) = cutting.take() {
                // Interrupt one real authenticated OS connection at an event,
                // leaving the listener and its ordinary server available.
                let (server, mut forwarding) = tokio::io::duplex(4096);
                tokio::spawn(async move {
                    tokio::select! {
                        _ = cutting => {},
                        _ = tokio::io::copy_bidirectional(&mut connection, &mut forwarding) => {},
                    }
                });
                crate::server::LocalConnection::new(server)
            } else {
                if let Some(reconnected) = reconnected.take() {
                    let _ = reconnected.send(());
                }
                connection
            }
        })
    });
    let (ready, readiness) = tokio::sync::oneshot::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let result = crate::server::serve(
            crate::server::Listener::Local(incoming.boxed()),
            crate::server::ServerConfig::new(config),
            |endpoint| {
                let _ = ready.send(owner.publish(endpoint));
            },
            async {
                let _ = stopped.await;
            },
        )
        .await;
        drop(owner);
        result
    });
    let result = async {
        readiness.await.map_err(storage)??;
        let directory = directory.connect().await?;
        let child = sessions::create(&directory, "observed-session", None).await?;
        let conversation = Conversation::open(child.clone());
        let mut events = conversation.observe(None).await?;
        loop {
            if matches!(events.next().await?, super::super::Event::Status(status) if matches!(status.phase, crate::agent::Phase::Idle)) {
                break;
            }
        }
        cut.send(()).map_err(|()| Error::Storage("connection already ended".into()))?;
        reconnecting.await.map_err(storage)?;
        sessions::archive(&child, "archive-after-reconnect").await?;
        loop {
            if matches!(events.next().await?, super::super::Event::Status(status) if matches!(status.phase, crate::agent::Phase::Done { outcome: crate::agent::AgentOutcome::Archived })) {
                break;
            }
        }
        Ok(())
    }.await;
    let _ = stop.send(());
    result.and(server.await.map_err(storage)?)
}
