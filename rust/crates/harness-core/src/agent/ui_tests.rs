use super::*;
use crate::{
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Ask(AtomicUsize);
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
                reserved_children: 0,
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
    let body = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes(path.id().to_bytes())),
            &mut None,
        )?,
        "reply.json".into(),
        "v1".into(),
        FileDescriptor::from_bytes(b"reply", "application/json", &mut None)?,
        "reply.json".into(),
        &mut None,
    )?;
    let message = |id: &str| -> Result<InboxRecord, Error> {
        Ok(InboxRecord::Message(Message {
            from: Sender::User {
                principal: "ui-test".into(),
            },
            id: IntentId::try_from(id)?,
            body: Box::new(body.clone()),
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
