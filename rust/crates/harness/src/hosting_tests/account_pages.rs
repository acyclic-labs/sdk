//! Signed peer discovery reads public account rows, never remote actor state.
use super::*;
use acyclic_actors::route::{NodeId, Paths};
use acyclic_harness_client::{PeerSession, SessionsScope};
use acyclic_stream::{CommitOutcome, CommitRequest, IdempotencyKey, StreamProvider as _};

#[tokio::test]
async fn account_peer_command_preserves_qualified_identity_credit_and_authority() -> Result {
    let bound = binding()?;
    let local = bound.agents().provider().clone();
    let account = Arc::new(Counted::new(MemoryStream::default()));
    let paths = Paths::new(StreamPath::new("account/routing")?);
    let root = AgentPath::root(&AgentId::from_bytes(u128::MAX.to_be_bytes()));
    let requester = bound.agents().actor(&root)?;
    let node = NodeId::new(b"device")?;
    let profile = crate::agent::SessionProfile::project(
        &crate::agent::Phase::Idle,
        "Public title ".repeat(128),
        &[],
        "Public goal",
        64,
    );
    let row = crate::agent::SessionRow::new(node, profile.clone());
    let id = AgentId::from_bytes([2; 16]);
    let peers = [
        ActorReference::new(b"account", b"agents", id.as_str().as_bytes())?,
        ActorReference::new(b"account", b"other-agents", id.as_str().as_bytes())?,
    ];
    let mut effects = Vec::new();
    for actor in peers.iter().chain([&requester]) {
        effects.push(row.effect(crate::agent::SessionPublication::row_path(actor)?)?);
    }
    assert!(matches!(
        account
            .commit(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: IdempotencyKey::unique(),
                effects,
            })
            .await?,
        CommitOutcome::Committed(_)
    ));
    // The admitted root is already in the account registry. Discovery must
    // not require a local actor checkpoint, directory, transcript or volume.
    let owner = Arc::new(Owner {
        peer_directory: Some(PeerDirectory {
            provider: account.clone(),
            paths,
        }),
        ..Default::default()
    });
    let server = reopen(bound, owner.clone(), root.clone()).await?;
    let connect = |peer: bool| -> Result<Client> {
        let client = Client::in_process_with_products(
            local.clone(),
            acyclic_stream::session::testing::authority(),
            None,
            server.clone(),
            Some(server.clone()),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            None,
        );
        client.register_credential(
            0,
            if peer {
                credentials(&[server.resource(), &server.peer_resource()])
            } else {
                credential(server.resource())
            },
        )?;
        Ok(client)
    };
    let client = connect(true)?;
    let command = |page| AgentCommand::Sessions {
        requester: None,
        scope: SessionsScope::Peers,
        page,
    };
    let before = account.requests();
    let local_before = local.requests();
    let AgentReply::Children { items, next } = call(
        &client,
        "agents",
        &command(AgentPageRequest::new(4, 64 * 1024, None)?),
    )
    .await??
    else {
        return Err("expected account peer rows".into());
    };
    assert_eq!(items.len(), 2);
    assert!(next.is_none());
    assert_eq!(items[0].agent, items[1].agent);
    let addresses = items
        .iter()
        .map(|item| {
            let peer = item.peer.as_ref().ok_or("peer metadata missing")?;
            assert_eq!(peer.profile, profile);
            assert!(!peer.online);
            assert!(item.status.is_none());
            Ok(peer.actor()?)
        })
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    assert_eq!(addresses, peers.into_iter().collect());
    let requests = account.requests().since(before);
    assert_eq!(requests.control(), 2);
    assert_eq!(requests.data(), 0);
    assert_eq!(local.requests().since(local_before).control(), 0);
    assert_eq!(local.requests().since(local_before).data(), 0);

    // Cut credit below two rows, including their canonical response framing.
    let full = AgentReply::Children {
        items: items.clone(),
        next: None,
    };
    let bytes = acyclic_harness_client::encoded_agent_reply_size(page_json_bytes(&full)?)?;
    let AgentReply::Children { items: first, next } = call(
        &client,
        "agents",
        &command(AgentPageRequest::new(4, u32::try_from(bytes - 1)?, None)?),
    )
    .await??
    else {
        return Err("expected credited account page".into());
    };
    assert_eq!(first.len(), 1);
    let AgentReply::Children { items: rest, .. } = call(
        &client,
        "agents",
        &command(AgentPageRequest::new(4, 64 * 1024, next)?),
    )
    .await??
    else {
        return Err("expected account continuation".into());
    };
    assert_eq!(rest.len(), 1);
    assert_ne!(
        first[0].peer.as_ref().map(|peer| &peer.address),
        rest[0].peer.as_ref().map(|peer| &peer.address)
    );

    let before = account.requests();
    assert!(!matches!(
        call(
            &connect(false)?,
            "agents",
            &command(AgentPageRequest::default())
        )
        .await,
        Ok(Ok(_))
    ));
    assert_eq!(account.requests().since(before).control(), 0);
    assert!(!matches!(
        call(
            &client,
            "agents",
            &AgentCommand::Sessions {
                requester: None,
                scope: SessionsScope::Owned,
                page: AgentPageRequest::default(),
            }
        )
        .await,
        Ok(Ok(_))
    ));
    owner.revoke_query.store(true, Ordering::SeqCst);
    assert!(!matches!(
        call(&client, "agents", &command(AgentPageRequest::default())).await,
        Ok(Ok(_))
    ));
    owner.denied.store(false, Ordering::SeqCst);
    let path = crate::agent::SessionPublication::row_path(&requester)?;
    assert!(matches!(
        account
            .commit(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: IdempotencyKey::unique(),
                effects: vec![row.changed(None)?.ok_or("removal missing")?.effect(path)?],
            })
            .await?,
        CommitOutcome::Committed(_)
    ));
    assert!(!matches!(
        call(&client, "agents", &command(AgentPageRequest::default())).await,
        Ok(Ok(_))
    ));
    // Metadata is data: a forged identity must also fail at the public decoder.
    let mut forged = items[0].clone();
    let peer: &mut PeerSession = forged.peer.as_mut().ok_or("peer metadata missing")?;
    peer.address = acyclic_auth::encode_component(&requester.encode());
    assert!(
        acyclic_harness_client::encode_agent_reply(
            &Ok(AgentReply::Children {
                items: vec![forged],
                next: None
            }),
            &command(AgentPageRequest::default()),
        )
        .is_err()
    );
    Ok(())
}
