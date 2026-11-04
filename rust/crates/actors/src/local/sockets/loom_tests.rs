//! Exactly one Close per socket, checked on production publication builders
//! using Stream's instrumented locks. Host admission and recovery belong to the
//! native conformance suite. Qualify this model's source mutations and restored
//! run with `python3 scripts/qualify-actors-sockets.py`.
use super::*;
use acyclic_stream::runtime::PlatformExecutor;
use acyclic_stream::{MemoryStream, Record};
use loom::future::block_on;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn records(provider: &MemoryStream, path: &StreamPath) -> Result<Vec<Record>> {
    Ok(provider
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from: 0,
            limit: PAGE,
            at_least: RevisionCut::default(),
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?)
}

/// Two closers race one message after the production Open effects commit.
async fn race() -> Result {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), StreamPath::new("loom/actors")?);
    let layout = Layout::new(&store)?;
    let target = Target {
        actor: ActorReference::new(b"account", b"sockets", b"loom")?,
        inbox: StreamPath::new("loom/inbox")?,
    };
    let socket = layout.socket("1-0")?;
    let epochs = vec![epoch_record(1, 1), epoch_record(1, 1)];
    let ledger = Ledger {
        segment: 1,
        allocated: 1,
        epochs: epochs.len() as u64,
    };
    assert!(
        commit(
            &store,
            &PlatformExecutor,
            layout.key(&[b"epochs"])?,
            vec![append(
                layout.epochs.clone(),
                epochs,
                vec![CommitCondition::Absent {
                    path: layout.epochs.clone(),
                }],
            )],
        )
        .await?
    );
    let open = SocketOpen {
        actor: target.actor.clone(),
        subscription: "inbox".into(),
        app_channel: None,
        subprotocols: Vec::new(),
        headers: Vec::new(),
        resume_after: 0,
        reconnected: false,
    };
    let invocation = open_invocation(socket.clone(), "1-0", &open)?;
    let mut effects = open_effects(
        &socket,
        &layout.epochs,
        &layout.segment(ledger.segment)?,
        ledger.epochs,
        OpenedWire {
            connection: "1-0".into(),
            actor: target.actor.encode().into(),
            inbox: target.inbox.as_str().into(),
        },
    );
    effects.push(append(
        target.inbox.clone(),
        vec![invocation.record(&store.path(&target.actor)?)?],
        Vec::new(),
    ));
    effects.push(store.activation(&target.actor));
    assert!(commit(&store, &PlatformExecutor, layout.key(&[b"open"])?, effects,).await?);
    let inbox = records(&provider, &target.inbox).await?;
    assert_eq!(inbox.len(), 1);
    let invocation = crate::invocation::Invocation::decode(&inbox[0])?
        .ok_or("Open admission omitted its invocation")?;
    assert_eq!(invocation.actor(), &target.actor);
    assert_eq!(invocation.subscription(), "inbox");
    assert_eq!(
        crate::sockets::input_kind(invocation.request()),
        Some("open")
    );
    let peer = loom::thread::spawn({
        let store = Store::new(Arc::clone(&provider), store.root().clone());
        let layout = layout.clone();
        let target = target.clone();
        move || {
            block_on(Box::pin(commit_close(
                &store,
                &PlatformExecutor,
                &layout,
                "1-0",
                &target,
                Some(ledger),
                &Close::new(1006, "peer went away", SocketCloseInitiator::Peer),
            )))
            .map_err(|error| error.to_string())
        }
    });
    commit_message(
        &store,
        &PlatformExecutor,
        &layout,
        "1-0",
        &target,
        1,
        input_record(
            "1-0",
            SocketInput::Message(SocketPayload::Text {
                text: "frame".into(),
            }),
        )?,
    )
    .await?;
    commit_close(
        &store,
        &PlatformExecutor,
        &layout,
        "1-0",
        &target,
        Some(ledger),
        &Close::new(1000, "actor closed", SocketCloseInitiator::Actor),
    )
    .await?;
    peer.join().map_err(|_| "peer closer panicked")??;
    let inbox = records(&provider, &target.inbox).await?;
    let kinds: Vec<&str> = inbox
        .iter()
        .skip(1)
        .map(|record| crate::sockets::input_kind(&record.value).unwrap_or("other"))
        .collect();
    assert_eq!(
        kinds.iter().filter(|kind| **kind == "close").count(),
        1,
        "socket delivered other than exactly one Close: {kinds:?}"
    );
    assert_eq!(
        kinds.last(),
        Some(&"close"),
        "an event followed the Close: {kinds:?}"
    );
    assert_eq!(records(&provider, &socket).await?.len(), 2);
    Ok(())
}

#[test]
fn concurrent_closes_and_messages_deliver_exactly_one_final_close() {
    loom::model(|| {
        // Keep the operation state in one pinned future rather than copying
        // each nested future through a by-value blocking helper.
        let result = block_on(Box::pin(race()));
        assert!(result.is_ok(), "{result:?}");
    });
}
