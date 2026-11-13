//! Original Stream publications prove the absence fence and its negative control.
use super::*;
use crate::agent::{
    AgentId, ForkAdopted, InboxRecord, IntentId, Message, PeerMembershipHead, Sender,
    create_root_with_peer,
};
use crate::conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, MemoryStream, RecordHeaders,
    StreamPath, StreamProvider as _, StreamProviderExt as _,
};

#[tokio::test]
async fn absent_refusal_fences_creation_of_a_disabled_recipient()
-> Result<(), Box<dyn std::error::Error>> {
    let reference = MemoryStream::default();
    let destination = MemoryStream::default();
    let recipient = AgentId::from_bytes([1; 16]);
    let sender = AgentPath::root(&AgentId::from_bytes([2; 16]));
    let created = create_root_with_peer(
        &IntentId::try_from("create")?,
        ForkAdopted {
            agent: recipient,
            parent: None,
            profile: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: Vec::new(),
            brief: None,
            policy: None,
        },
        false,
    )?;
    assert!(matches!(
        reference.commit(created.request.clone()).await?,
        CommitOutcome::Committed(_)
    ));
    let membership = reference
        .head(created.child.peer_membership()?)
        .await?
        .last
        .ok_or("membership missing")?;
    let witness = PeerMembershipHead {
        tail: 1,
        commit: *membership.commit_id.as_bytes(),
    };
    let input = InboxRecord::Message(Message {
        from: Sender::Agent {
            path: sender.clone(),
        },
        id: IntentId::try_from("message")?,
        re: None,
        body: Box::new(FileRef::new(
            VolumeRef::new(
                crate::resources::ProviderRef::new(
                    "test".into(),
                    "filesystem".into(),
                    "1".into(),
                    &mut None,
                )?,
                "private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([2; 16])),
                &mut None,
            )?,
            "message.md".into(),
            "version-1".into(),
            FileDescriptor::from_bytes(b"message", "text/plain", &mut None)?,
            "message.md".into(),
            &mut None,
        )?),
        attachments: None,
    });
    let admission = PeerAdmission {
        sender: witness.clone(),
        recipient: witness,
    };
    let mut request = crate::agent::send(
        &created.child,
        &Sender::Agent {
            path: sender.clone(),
        },
        &input,
    )?;
    let CommitMutation::Append { records, .. } = &mut request.effects[0].mutations[0] else {
        return Err("inbox append missing".into());
    };
    records[0].headers = RecordHeaders::new(
        Vec::new(),
        vec![acyclic_stream::Attribute {
            name: ADMISSION.into(),
            value: admission.encode()?.into(),
        }],
    )?;
    let actor = |path: &AgentPath| {
        ActorReference::new(b"account", b"agents", path.id().as_str().as_bytes())
    };
    let envelope = route::Envelope {
        source: route::NodeId::new(b"source")?,
        sender: actor(&sender)?,
        to: actor(&created.child)?,
        request,
    };
    let peer = PeerRoute::parse(&envelope)?;
    let routing = Routing {
        paths: route::Paths::new(StreamPath::new("routing")?),
        node: route::NodeId::new(b"destination")?,
    };
    let account = MemoryStream::default();
    envelope
        .publish_relay(&account, &routing.paths, &routing.node, Vec::new())
        .await?;
    let relay = account
        .record_at(routing.paths.relay(&routing.node)?, 0)
        .await?
        .ok_or("relay missing")?;
    let receipt = route::RelayReceipt {
        commit_id: relay.commit_id,
        sequence: relay.sequence,
    };
    assert!(
        !peer
            .admission
            .recipient
            .retained(&destination, &created.child)
            .await?
    );
    let refusal = peer.absent_request(&routing, &receipt, peer.delivery_request(&receipt)?)?;

    // Creation wins after the missing-witness read but before the conditional
    // refusal. The adversarial future head now matches, but it is disabled.
    assert!(matches!(
        destination.commit(created.request).await?,
        CommitOutcome::Committed(_)
    ));
    assert!(
        peer.admission
            .recipient
            .validate(&destination, &created.child)
            .await
            .is_err()
    );
    assert!(
        matches!(
            destination.commit(refusal.clone()).await?,
            CommitOutcome::Conflict(_)
        ),
        "absence must be established at the original refusal commit"
    );
    assert!(
        destination
            .inspect_idempotency(refusal.idempotency_key.clone())
            .await?
            .is_none()
    );
    assert_eq!(destination.tail(created.child.inbox()?).await?, 1);

    // Removing only the hard absence condition reproduces the bypass: the
    // ordinary soft effect sees a matching disabled head and admits the input.
    let mut unguarded: CommitRequest = refusal;
    for effect in &mut unguarded.effects {
        effect.conditions.retain(|condition| !matches!(condition, CommitCondition::Absent { path } if path == &created.child.inbox().expect("valid inbox")));
    }
    assert!(matches!(
        destination.commit(unguarded).await?,
        CommitOutcome::Committed(_)
    ));
    assert_eq!(
        destination.tail(created.child.inbox()?).await?,
        2,
        "negative control must admit input to the disabled root"
    );
    Ok(())
}
