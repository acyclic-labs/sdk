use super::*;
use crate::{
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Ask(AtomicUsize);

#[test]
fn peer_model_input_preserves_attribution_and_pinned_attachments()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::model::{FileProjectionPolicy, ModelContent, ModelContentPart};

    let peer = AgentPath::root(&AgentId::from_bytes([2; 16]));
    let body = message_file(&peer, "body.md")?;
    let attachment = message_file(&peer, "attachments.json")?;
    let from = Sender::Agent { path: peer.clone() };
    let message = Message {
        from: from.clone(),
        id: IntentId::try_from("peer-message")?,
        body: Box::new(body.clone()),
        re: Some(IntentId::try_from("original-message")?),
        attachments: Some(Box::new(attachment.clone())),
    };
    for (record, steering) in [
        (InboxRecord::Message(message.clone()), true),
        (InboxRecord::FollowUp(message), false),
    ] {
        let queued = turn_input(7, InboxRecord::decode(&record.encode()?)?)
            .ok_or("peer message did not become turn input")?;
        assert_eq!((queued.inbox, queued.steering), (7, steering));
        let mut input = queued.input;
        let ModelContent::Parts(parts) = model_input(&input)? else {
            return Err("missing peer envelope".into());
        };
        let [ModelContentPart::Notices { notices }, file, manifest] = parts.as_slice() else {
            return Err("peer envelope must precede the body and attachments".into());
        };
        let [notice] = notices.as_slice() else {
            return Err("one attributed peer input expected".into());
        };
        assert_eq!(notice.source, peer.as_str());
        assert_eq!(notice.kind, "peer.message");
        assert_eq!(notice.body["from"], serde_json::to_value(&from)?);
        assert_eq!(notice.body["id"], "peer-message");
        assert_eq!(notice.body["re"], "original-message");
        assert_eq!(
            file,
            &ModelContentPart::File {
                file: body.clone(),
                policy: FileProjectionPolicy::BoundedFull,
            }
        );
        assert_eq!(
            manifest,
            &ModelContentPart::File {
                file: attachment.clone(),
                policy: FileProjectionPolicy::Reference,
            }
        );
        let TurnInput::Message { from, .. } = &mut input else {
            return Err("message changed type".into());
        };
        *from = Sender::User {
            principal: "authenticated-user".into(),
        };
        let ModelContent::Parts(user_parts) = model_input(&input)? else {
            return Err("missing user attachments".into());
        };
        assert_eq!(user_parts.as_slice(), &parts[1..]);
    }
    Ok(())
}

impl TurnDriver for Ask {
    fn step<'a>(
        &'a self,
        _: &'a AgentState,
        _: u64,
        _: Option<Queued>,
        _: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let call = self.0.fetch_add(1, Ordering::SeqCst);
            Ok(TurnStep {
                checkpoint: None,

                outs: Vec::new(),
                wait: if call == 0 {
                    Wait::User("ask.md".into())
                } else {
                    Wait::Input
                },
                drops: None,
            })
        })
    }
}

#[tokio::test]
async fn ui_reply_roundtrip_clears_attention_on_inbox_delivery()
-> Result<(), Box<dyn std::error::Error>> {
    let path = AgentPath::root(&AgentId::from_bytes([1; 16]));
    let adopted = ForkAdopted {
        agent: path.id(),
        parent: None,
        profile: None,
        forked_at: 0,
        conversation_tail: 0,
        checkpoint: None,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    };
    let driver = Ask(AtomicUsize::new(0));
    let stop = acyclic_actors::guest::CancellationToken::new();
    let first = step(
        &driver,
        &path,
        None,
        (1, 0),
        Wake::Inbox {
            sequence: 0,
            record: InboxRecord::ForkAdopted(Box::new(adopted)),
        },
        stop.clone(),
    )
    .await?;
    let body = message_file(&path, "reply.json")?;
    let message = |id: &str| -> Result<InboxRecord, Error> {
        Ok(InboxRecord::Message(Message {
            from: Sender::User {
                principal: "ui-test".into(),
            },
            id: IntentId::try_from(id)?,
            body: Box::new(body.clone()),
            re: None,
            attachments: None,
        }))
    };
    let waiting = step(
        &driver,
        &path,
        Some(first.state),
        (2, 0),
        Wake::Inbox {
            sequence: 1,
            record: message("ask")?,
        },
        stop.clone(),
    )
    .await?;
    assert_eq!(
        waiting.state.phase,
        Phase::WaitingUser {
            path: "ask.md".into()
        }
    );
    assert!(!waiting.state.phase.in_turn());
    let status = waiting.status.ok_or("attention status")?;
    assert!(status.phase.attention());
    let record = status.append_record()?;
    assert!(
        record
            .headers
            .attributes()
            .iter()
            .any(|a| a.name == "attention" && a.value.as_slice() == b"true")
    );
    let next = step(
        &driver,
        &path,
        Some(waiting.state),
        (3, 0),
        Wake::Inbox {
            sequence: 2,
            record: message("reply")?,
        },
        stop,
    )
    .await?;
    assert_eq!(next.state.phase, Phase::Idle);
    assert!(!next.status.ok_or("cleared status")?.phase.attention());
    assert_eq!(driver.0.load(Ordering::SeqCst), 2);
    Ok(())
}

fn message_file(path: &AgentPath, name: &str) -> Result<FileRef, Box<dyn std::error::Error>> {
    let body = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes(path.id().to_bytes())),
            &mut None,
        )?,
        name.into(),
        "v1".into(),
        FileDescriptor::from_bytes(b"reply", "application/json", &mut None)?,
        name.into(),
        &mut None,
    )?;
    Ok(body)
}
