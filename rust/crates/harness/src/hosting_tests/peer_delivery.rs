//! Prepared external messages use authenticated session identity and the one inbox writer.
use super::*;
use crate::agent::{InboxRecord, Sender, SessionPublication, SessionRow};
use acyclic_stream::{ReadRequest, StreamProvider as _};
use futures::TryStreamExt as _;

#[tokio::test]
async fn authenticated_peer_delivery_pins_attribution_opt_out_and_exact_retry() -> Result {
    let routing = crate::agent::Routing {
        paths: acyclic_actors::route::Paths::new(acyclic_stream::StreamPath::new(
            "account/routing",
        )?),
        node: acyclic_actors::route::NodeId::new(b"local")?,
    };
    let bound = binding_routed(Arc::new(Never), Default::default(), Some(routing.clone()))?;
    let stream = bound.agents().provider().clone();
    let owner = Arc::new(Owner::default());
    owner.sessions.store(true, Ordering::SeqCst);
    let mut servers = Vec::new();
    for index in [1, 2] {
        let agent = AgentId::from_bytes([index; 16]);
        let Serving::Served(server) = serve(
            bound.clone(),
            owner.clone(),
            &IntentId::try_from(format!("session-{index}"))?,
            RootRequest {
                peer: true,
                fresh: vec![WorkspaceEntry::initial_volume(
                    crate::agent::WorkspaceRole::Private,
                    PRIVATE_VOLUME.into(),
                    VolumeRef::new(
                        provider()?,
                        "private".into(),
                        VolumeClass::AgentPrivate,
                        VolumeOwner::Agent(crate::AgentId::from_bytes(agent.to_bytes())),
                        &mut None,
                    )?,
                )],
                agent,
                retained: Vec::new(),
                brief: None,
                policy: None,
            },
        )
        .await?
        else {
            return Err("session creation was uncertain".into());
        };
        servers.push(server);
    }
    let server = &servers[0];
    let sender = server.root().clone();
    let recipient = servers[1].root().clone();
    let row_path = SessionPublication::row_path(&bound.agents().actor(&recipient)?)?;
    let client = Client::in_process_with_products(
        stream.clone(),
        acyclic_stream::session::testing::authority(),
        None,
        server.clone(),
        Some(server.clone()),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(
        0,
        credentials(&[server.resource(), &server.session_resource()]),
    )?;
    let outbox = routing.paths.outbox(&routing.node)?;
    let before_local_work = stream.head(outbox.clone()).await?;
    let note = "{\"note\":\"local file\"}";
    assert!(matches!(
        call(
            &client,
            "agents",
            &AgentCommand::PublishAgentFiles {
                agent: sender.clone(),
                operation: crate::OperationId::new(),
                changes: vec![crate::client_files::ClientFileChange {
                    path: "notes.json".into(),
                    text: Some(note.into()),
                    expected: None,
                }],
            }
        )
        .await??,
        AgentReply::ClientFilesPublished { .. }
    ));
    let AgentReply::FilePreviews { snapshot } = call(
        &client,
        "agents",
        &AgentCommand::AgentFilePreviews {
            agent: sender.clone(),
            paths: vec!["notes.json".into()],
            after: None,
            bytes: 4096,
        },
    )
    .await??
    else {
        return Err("local file preview returned another reply".into());
    };
    assert_eq!(
        snapshot.files.get("notes.json").and_then(Option::as_deref),
        Some(note)
    );
    let prepare = AgentCommand::PreparePeer {
        sender: sender.clone(),
        agent: recipient.clone(),
    };
    let AgentReply::PeerAdmission {
        admission: Some(admission),
    } = call(&client, "agents", &prepare).await??
    else {
        return Err("member roots were not admitted".into());
    };
    let command = AgentCommand::Prompt {
        agent: recipient.clone(),
        id: IntentId::try_from("external-message")?,
        body: ModelContent::Text("Please report your current status.".into()),
        peer: Some((sender.clone(), Some(admission.clone()))),
    };
    // Full directory authority does not turn absent membership into a peer
    // grant. Only a sender's own descendants may use tree attribution.
    for target in [recipient.clone(), sender.clone()] {
        let unadmitted = AgentCommand::Prompt {
            agent: target,
            id: IntentId::try_from("missing-admission")?,
            body: ModelContent::Text("Unadmitted input".into()),
            peer: Some((sender.clone(), None)),
        };
        assert!(matches!(
            call(&client, "agents", &unadmitted).await?,
            Err(_)
        ));
    }
    assert_eq!(stream.tail(recipient.inbox()?).await?, 1);
    // A sender's tree credential admits its own attributed peer input without
    // acquiring control over either the directory or the recipient session.
    client.register_credential(0, credential(server.resource()))?;
    assert!(!matches!(
        call(
            &client,
            "agents",
            &AgentCommand::PreparePeer {
                sender: recipient.clone(),
                agent: sender.clone(),
            }
        )
        .await,
        Ok(Ok(_))
    ));
    assert!(matches!(
        call(&client, "agents", &prepare).await??,
        AgentReply::PeerAdmission { admission: Some(_) }
    ));
    for forbidden in [
        AgentCommand::Prompt {
            agent: recipient.clone(),
            id: IntentId::try_from("foreign-user-input")?,
            body: ModelContent::Text("Unattributed input".into()),
            peer: None,
        },
        AgentCommand::Archive {
            agent: recipient.clone(),
            id: IntentId::try_from("foreign-archive")?,
        },
        AgentCommand::SetPeerMembership {
            agent: recipient.clone(),
            id: IntentId::try_from("foreign-opt-out")?,
            enabled: false,
        },
        AgentCommand::AgentFiles {
            agent: recipient.clone(),
            paths: vec!["ui/title".into()],
            after: None,
        },
    ] {
        assert!(!matches!(
            call(&client, "agents", &forbidden).await,
            Ok(Ok(_))
        ));
    }
    // Independent directory serving is not needed by the append-only path.
    owner.sessions.store(false, Ordering::SeqCst);
    let first = call(&client, "agents", &command).await??;
    let after_local_work = stream.head(outbox.clone()).await?;
    assert_eq!(
        after_local_work.tail, before_local_work.tail,
        "local files and same-node sends published router events"
    );
    assert_eq!(
        after_local_work
            .last
            .as_ref()
            .map(|record| record.commit_id),
        before_local_work
            .last
            .as_ref()
            .map(|record| record.commit_id)
    );
    assert!(matches!(
        first,
        AgentReply::Delivered {
            delivery: Delivered::Committed { inbox: 1 }
        }
    ));
    let mut records = stream
        .read(ReadRequest {
            key: None,
            path: recipient.inbox()?,
            from: 1,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    let record = records.try_next().await?.ok_or("peer input missing")?;
    let InboxRecord::Message(message) = InboxRecord::decode(&record.value)? else {
        return Err("delivery was not a message".into());
    };
    assert_eq!(
        message.from,
        Sender::Agent {
            path: sender.clone()
        }
    );
    assert!(message.attachments.is_none());
    drop(records);
    // Opt-out fences a new intent prepared before it, but an acknowledged
    // message still recovers its original receipt without a second append.
    bound
        .agents()
        .set_peer_membership(
            &recipient,
            &Sender::User {
                principal: "owner".into(),
            },
            &IntentId::try_from("off")?,
            false,
        )
        .await?;
    let removed = stream
        .head(row_path.clone())
        .await?
        .last
        .ok_or("row removal missing")?;
    let removed_row = SessionRow::decode(&removed)?;
    assert!(removed_row.profile.is_none());
    let membership = stream
        .head(recipient.peer_membership()?)
        .await?
        .last
        .ok_or("membership missing")?;
    assert_eq!(membership.commit_id, removed.commit_id);
    let outgoing = stream
        .head(routing.paths.outbox(&routing.node)?)
        .await?
        .last
        .ok_or("removal outbox missing")?;
    assert!(
        outgoing.sequence >= before_local_work.tail,
        "the outbox counter failed to observe an actual membership publication"
    );
    assert_eq!(outgoing.commit_id, removed.commit_id);
    assert!(matches!(
        acyclic_actors::route::Publication::decode(&outgoing.value)?,
        acyclic_actors::route::Publication::Call(_)
    ));
    assert!(matches!(
        call(&client, "agents", &prepare).await??,
        AgentReply::PeerAdmission { admission: None }
    ));
    assert!(matches!(
        call(&client, "agents", &command).await??,
        AgentReply::Delivered {
            delivery: Delivered::Committed { inbox: 1 }
        }
    ));
    let AgentCommand::Prompt { body, .. } = &command else {
        unreachable!()
    };
    let stale = AgentCommand::Prompt {
        agent: recipient.clone(),
        id: IntentId::try_from("stale-admission")?,
        body: body.clone(),
        peer: Some((sender, Some(admission))),
    };
    assert!(matches!(call(&client, "agents", &stale).await?, Err(_)));
    assert_eq!(stream.tail(recipient.inbox()?).await?, 2);
    // A changed body under the same intent cannot append or overwrite input.
    let mut changed = command;
    if let AgentCommand::Prompt { body, .. } = &mut changed {
        *body = ModelContent::Text("Different content".into());
    }
    assert!(matches!(call(&client, "agents", &changed).await?, Err(_)));
    assert_eq!(stream.tail(recipient.inbox()?).await?, 2);
    let user = Sender::User {
        principal: "owner".into(),
    };
    let off = IntentId::try_from("off")?;
    let on = IntentId::try_from("on")?;
    bound
        .agents()
        .set_peer_membership(&recipient, &user, &off, false)
        .await?;
    assert_eq!(stream.tail(row_path.clone()).await?, removed_row.rev);
    assert!(
        bound
            .agents()
            .set_peer_membership(&recipient, &user, &off, true)
            .await
            .is_err(),
        "changed membership reused an accepted intent"
    );
    bound
        .agents()
        .set_peer_membership(&recipient, &user, &on, true)
        .await?;
    let rejoined = stream
        .head(row_path.clone())
        .await?
        .last
        .ok_or("rejoin missing")?;
    let rejoined_row = SessionRow::decode(&rejoined)?;
    assert!(rejoined_row.profile.is_some());
    assert_eq!(rejoined_row.rev, removed_row.rev + 1);
    assert_eq!(
        stream.tail(recipient.inbox()?).await?,
        2,
        "membership controls woke the inbox"
    );
    bound
        .agents()
        .set_peer_membership(&recipient, &user, &off, false)
        .await?;
    assert_eq!(
        stream
            .head(row_path.clone())
            .await?
            .last
            .ok_or("row missing")?
            .commit_id,
        rejoined.commit_id,
        "old opt-out replay removed a later rejoin"
    );
    assert!(
        bound
            .agents()
            .set_peer_membership(
                &recipient,
                &Sender::Agent {
                    path: recipient.clone()
                },
                &IntentId::try_from("agent-opt-out")?,
                false,
            )
            .await
            .is_err()
    );
    let archive = IntentId::try_from("archive")?;
    let first = bound.agents().archive(&recipient, &user, &archive).await?;
    let archived = stream
        .head(row_path.clone())
        .await?
        .last
        .ok_or("archive row missing")?;
    let archived_row = SessionRow::decode(&archived)?;
    assert!(archived_row.profile.is_none());
    assert_eq!(archived_row.rev, rejoined_row.rev + 1);
    let closed = stream
        .head(recipient.closed()?)
        .await?
        .last
        .ok_or("closed marker missing")?;
    assert_eq!(closed.commit_id, archived.commit_id);
    assert_eq!(
        bound.agents().archive(&recipient, &user, &archive).await?,
        first
    );
    bound
        .agents()
        .set_peer_membership(&recipient, &user, &on, true)
        .await?;
    assert_eq!(
        stream
            .head(row_path.clone())
            .await?
            .last
            .ok_or("archive row missing")?
            .commit_id,
        archived.commit_id,
        "old rejoin replay resurrected an archived row"
    );
    assert!(
        bound
            .agents()
            .set_peer_membership(&recipient, &user, &IntentId::try_from("new-rejoin")?, true)
            .await
            .is_err()
    );
    assert_eq!(stream.tail(row_path).await?, archived_row.rev);
    Ok(())
}
