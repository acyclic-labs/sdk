//! Prepared external messages use authenticated session identity and the one inbox writer.
use super::*;
use crate::agent::{InboxRecord, Sender};
use acyclic_stream::{ReadRequest, StreamProvider as _};
use futures::TryStreamExt as _;

#[tokio::test]
async fn authenticated_peer_delivery_pins_attribution_opt_out_and_exact_retry() -> Result {
    let bound = binding()?;
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
    let client = Client::in_process_with_products(
        stream.clone(),
        acyclic_stream::session::testing::authority(),
        None,
        server.clone(),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(
        0,
        credentials(&[server.resource(), &server.session_resource()]),
    )?;
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
        peer: Some((sender.clone(), admission.clone())),
    };
    // A sender's tree credential must not silently acquire directory access.
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
    assert!(!matches!(
        call(&client, "agents", &command).await,
        Ok(Ok(_))
    ));
    assert_eq!(stream.tail(recipient.inbox()?).await?, 1);
    client.register_credential(
        0,
        credentials(&[server.resource(), &server.session_resource()]),
    )?;
    let first = call(&client, "agents", &command).await??;
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
        peer: Some((sender, admission)),
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
    Ok(())
}
