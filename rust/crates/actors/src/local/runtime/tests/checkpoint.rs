use super::*;
use crate::guest::{CancellationToken, CodecIdentity};
use crate::invocation::Invocation;

struct Persists {
    manifest: Manifest,
    checkpoint: Checkpoint,
}

impl Handler for Persists {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        _checkpoint: Option<&'a Checkpoint>,
        _cancellation: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            let invocation = delivery
                .invocation()?
                .ok_or_else(|| crate::guest::GuestError::Handler("missing invocation".into()))?;
            Ok(Transition {
                response: Some(invocation.request().clone()),
                checkpoint: self.checkpoint.clone(),
                alarm: AlarmChange::Keep,
                effects: Vec::new(),
                participant: None,
            })
        })
    }
}

/// Logical checkpoints cross record and command boundaries. Replacement and
/// forks preserve historical invocation receipts and exact bytes after reopen.
#[tokio::test]
async fn logical_checkpoint_crosses_record_boundary_and_reopens() -> TestResult {
    let directory = tempfile::tempdir()?;
    let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let address = actor("checkpoint-boundary")?;
    let store = Store::new(Arc::clone(&provider), root()?);
    let mut create = PreparedPublish::foreign(key("checkpoint-boundary/create")?);
    for effect in store.create(
        &address,
        BTreeMap::from([(
            "inbox".into(),
            Subscription {
                path: StreamPath::new("checkpoint-boundary/inbox")?,
                cursor: 0,
            },
        )]),
        None,
    )? {
        create.join(effect)?;
    }
    store.submit(create).await?;
    let mut history = Vec::new();
    for (index, length) in [
        (0u8, acyclic_stream::MAX_RECORD_BYTES + 1),
        (1u8, acyclic_stream::MAX_COMMAND_BYTES + 1),
    ] {
        let checkpoint = Checkpoint {
            codec: CodecIdentity {
                name: "checkpoint-boundary".into(),
                version: "1".into(),
            },
            bytes: vec![0x35 + index; length].into(),
        };
        let manifest = Manifest {
            code: Package::new()?.manifest().code.clone(),
            codec: checkpoint.codec.clone(),
            subscriptions: vec!["inbox".into()],
            interrupts: Vec::new(),
            bindings: Vec::new(),
            limits: crate::domain::ActorLimits::new(None, None, None)?,
        };
        let runtime = Runtime::new(
            Store::new(Arc::clone(&provider), root()?),
            Arc::new(Persists {
                manifest,
                checkpoint: checkpoint.clone(),
            }),
            Arc::new(SystemClock),
        );
        runtime.track(address.clone())?;
        let invocation = Invocation::new(
            StreamPath::new("checkpoint-boundary/caller")?,
            address.clone(),
            "inbox".into(),
            format!("persist-{index}").into_bytes().into(),
            format!("original response {index}").into_bytes().into(),
        )?;
        let ticket = store.enqueue_invoke(&invocation).await?;
        let result = runtime.drain_actor(&address).await;
        assert!(
            result.is_ok(),
            "unbounded logical checkpoint must publish independently of record size: {result:?}"
        );
        let completed = store.wait_invocation(ticket.clone()).await?;
        assert_eq!(completed.response().as_ref(), invocation.request().as_ref());
        assert_eq!(
            completed.receipt().state().commit_id,
            completed.receipt().terminal().commit_id
        );
        history.push((invocation, ticket, checkpoint, completed.receipt().clone()));
    }
    // A different store observes the original provider, without owning a
    // runtime or the author's latest retained publication.
    let observer = Store::new(Arc::clone(&provider), root()?);
    for (invocation, ticket, checkpoint, receipt) in &history {
        let original = observer.wait_invocation(ticket.clone()).await?;
        assert_eq!(original.response().as_ref(), invocation.request().as_ref());
        assert_eq!(original.receipt(), receipt);
        assert_eq!(
            State::decode(&original.receipt().state().value)?.checkpoint(),
            Some(checkpoint)
        );
    }
    let checkpoint = history.last().ok_or("missing checkpoint")?.2.clone();
    let mut descendants = Vec::new();
    for (name, parent) in [
        ("branch", address.clone()),
        ("leaf", actor("branch")?),
        ("sibling", address.clone()),
    ] {
        let child = actor(name)?;
        let parent = observer.load(&parent).await?;
        let mut fork = PreparedPublish::foreign(key(&format!("checkpoint-boundary/{name}"))?);
        for effect in store.fork(
            &parent,
            &child,
            BTreeMap::from([(
                "inbox".into(),
                Subscription {
                    path: StreamPath::new(format!("checkpoint-boundary/{name}/inbox"))?,
                    cursor: 0,
                },
            )]),
        )? {
            fork.join(effect)?;
        }
        store.submit(fork).await?;
        let inherited = observer.load(&child).await?;
        let state = inherited.state().ok_or("missing fork state")?;
        assert_eq!(state.checkpoint(), Some(&checkpoint));
        let claimed = store
            .claim(
                &inherited,
                state.epoch() + 1,
                key(&format!("checkpoint-boundary/{name}/claim"))?,
            )
            .await?;
        assert_eq!(
            claimed.state().and_then(State::checkpoint),
            Some(&checkpoint)
        );
        descendants.push(child);
    }
    let seeded = actor("seeded-checkpoint")?;
    let mut create = PreparedPublish::foreign(key("checkpoint-boundary/seeded")?);
    for effect in store.create(&seeded, BTreeMap::new(), Some(checkpoint.clone()))? {
        create.join(effect)?;
    }
    store.submit(create).await?;
    descendants.push(seeded);
    drop(observer);
    drop(store);
    drop(provider);
    let reopened = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let store = Store::new(reopened, root()?);
    let restored = store.load(&address).await?;
    let state = restored.state().ok_or("missing reopened state")?;
    assert_eq!(state.cursor("inbox"), Some(2));
    assert_eq!(state.checkpoint(), Some(&checkpoint));
    for child in descendants {
        let restored = store.load(&child).await?;
        assert_eq!(
            restored.state().and_then(State::checkpoint),
            Some(&checkpoint)
        );
    }
    for (invocation, _, checkpoint, receipt) in history {
        // Keyed reconciliation reissues the original input's read floor in
        // this provider incarnation, without executing either handler again.
        let ticket = store.enqueue_invoke(&invocation).await?;
        let original = store.wait_invocation(ticket).await?;
        assert_eq!(original.receipt(), &receipt);
        assert_eq!(original.response().as_ref(), invocation.request().as_ref());
        assert_eq!(
            State::decode(&original.receipt().state().value)?.checkpoint(),
            Some(&checkpoint)
        );
    }
    Ok(())
}

/// Explicit policy boundaries use the original invocation's terminal event;
/// accepted and refused candidates retain their exact state cuts on reopen.
#[tokio::test]
async fn checkpoint_policy_settles_original_invocation_and_reopens() -> TestResult {
    let directory = tempfile::tempdir()?;
    let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let store = Store::new(Arc::clone(&provider), root()?);
    let candidate = Checkpoint {
        codec: CodecIdentity {
            name: "checkpoint-policy".into(),
            version: "1".into(),
        },
        bytes: Bytes::from_static(b"updated"),
    };
    let previous = Checkpoint {
        bytes: Bytes::from_static(b"old"),
        ..candidate.clone()
    };
    let policies = std::iter::once(None).chain((1..=8).map(Some));
    let mut restored = Vec::new();
    for maximum in policies {
        let name = format!("policy-{}", maximum.unwrap_or_default());
        let address = actor(&name)?;
        let mut create = PreparedPublish::foreign(key(&format!("{name}/create"))?);
        for effect in store.create(
            &address,
            BTreeMap::from([(
                "inbox".into(),
                Subscription {
                    path: StreamPath::new(format!("{name}/inbox"))?,
                    cursor: 0,
                },
            )]),
            Some(previous.clone()),
        )? {
            create.join(effect)?;
        }
        store.submit(create).await?;
        let runtime = Runtime::new(
            Store::new(Arc::clone(&provider), root()?),
            Arc::new(Persists {
                manifest: Manifest {
                    code: Package::new()?.manifest().code.clone(),
                    codec: candidate.codec.clone(),
                    subscriptions: vec!["inbox".into()],
                    interrupts: Vec::new(),
                    bindings: Vec::new(),
                    limits: crate::domain::ActorLimits::new(None, None, maximum)?,
                },
                checkpoint: candidate.clone(),
            }),
            Arc::new(SystemClock),
        );
        runtime.track(address.clone())?;
        let invocation = Invocation::new(
            StreamPath::new("checkpoint-policy/caller")?,
            address.clone(),
            "inbox".into(),
            Bytes::from_static(b"policy"),
            Bytes::from_static(b"original response"),
        )?;
        let ticket = store.enqueue_invoke(&invocation).await?;
        let drained = runtime.drain_actor(&address).await;
        assert!(
            drained.is_ok(),
            "checkpoint policy lost original terminal: maximum={maximum:?}, {drained:?}"
        );
        let refused = maximum.is_some_and(|limit| limit < candidate.bytes.len() as u64);
        let completion = store.wait_invocation(ticket.clone()).await;
        let receipt = match completion {
            Err(Error::InvocationFailed { source, receipt }) if refused => {
                assert_eq!(
                    source,
                    crate::guest::GuestError::CheckpointLimit {
                        maximum: maximum.ok_or("missing explicit checkpoint policy")?,
                    },
                    "invocation failure codec lost original typed policy"
                );
                match store.invoke(&invocation).await {
                    Err(Error::InvocationFailed {
                        source: repeated,
                        receipt: original,
                    }) => {
                        assert_eq!(repeated, source);
                        assert_eq!(original, receipt);
                    }
                    _ => return Err("policy failure lost its original replay".into()),
                }
                *receipt
            }
            Ok(completed) if !refused => {
                assert_eq!(completed.response().as_ref(), invocation.request().as_ref());
                completed.receipt().clone()
            }
            _ => return Err("checkpoint policy changed its original outcome".into()),
        };
        assert_eq!(receipt.ticket(), &ticket);
        assert_eq!(receipt.state().commit_id, receipt.terminal().commit_id);
        let state = State::decode(&receipt.state().value)?;
        assert_eq!(state.cursor("inbox"), Some(1));
        let expected = if refused { &previous } else { &candidate };
        assert_eq!(state.checkpoint(), Some(expected));
        restored.push((address, expected.clone()));
    }
    drop(store);
    drop(provider);
    let reopened = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let store = Store::new(reopened, root()?);
    for (address, expected) in restored {
        let snapshot = store.load(&address).await?;
        let state = snapshot.state().ok_or("missing reopened policy state")?;
        assert_eq!(state.cursor("inbox"), Some(1));
        assert_eq!(state.checkpoint(), Some(&expected));
    }
    Ok(())
}

/// The original platform deadline ends an invocation with its exact policy
/// cause, retaining the previous checkpoint and the same durable input cut.
#[tokio::test(start_paused = true)]
async fn handler_deadline_settles_original_invocation_and_reopens() -> TestResult {
    // Construct the same deadline adapter used below without registering a
    // platform timer. Its frame belongs to the original delivery future.
    let mut deadline = None;
    let allocation = acyclic_test_support::measure(|| {
        deadline = Some(std::hint::black_box(super::super::clock::sleep(1)));
    });
    assert_eq!(
        allocation.count_total, 0,
        "handler deadline adapter allocated a separate frame"
    );
    drop(deadline);
    let directory = tempfile::tempdir()?;
    let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let store = Store::new(Arc::clone(&provider), root()?);
    let previous = Checkpoint {
        codec: conformance::codec(),
        bytes: Bytes::from_static(b"previous checkpoint"),
    };
    let mut restored = Vec::new();
    for maximum_millis in [1, 50, 10_000] {
        let name = format!("deadline-{maximum_millis}");
        let address = actor(&name)?;
        let mut create = PreparedPublish::foreign(key(&format!("{name}/create"))?);
        for effect in store.create(
            &address,
            BTreeMap::from([(
                "inbox".into(),
                Subscription {
                    path: StreamPath::new(format!("{name}/inbox"))?,
                    cursor: 0,
                },
            )]),
            Some(previous.clone()),
        )? {
            create.join(effect)?;
        }
        store.submit(create).await?;
        let runtime = Runtime::new(
            Store::new(Arc::clone(&provider), root()?),
            Arc::new(Limited::new(maximum_millis, None, true)?),
            Arc::new(SystemClock),
        );
        runtime.track(address.clone())?;
        let invocation = Invocation::new(
            StreamPath::new("handler-deadline/caller")?,
            address.clone(),
            "inbox".into(),
            Bytes::from_static(b"deadline"),
            Bytes::from_static(b"original request"),
        )?;
        let ticket = store.enqueue_invoke(&invocation).await?;
        let drained = runtime.drain_actor(&address).await;
        assert!(
            drained.is_ok(),
            "handler deadline lost original terminal: maximum={maximum_millis}, {drained:?}"
        );
        let Err(Error::InvocationFailed { source, receipt }) =
            store.wait_invocation(ticket.clone()).await
        else {
            return Err("handler deadline lost its original failure".into());
        };
        assert_eq!(
            source,
            crate::guest::GuestError::HandlerTimeout { maximum_millis }
        );
        assert_eq!(receipt.ticket(), &ticket);
        assert_eq!(receipt.state().commit_id, receipt.terminal().commit_id);
        let state = State::decode(&receipt.state().value)?;
        assert_eq!(state.cursor("inbox"), Some(1));
        assert_eq!(state.checkpoint(), Some(&previous));
        match store.invoke(&invocation).await {
            Err(Error::InvocationFailed {
                source: repeated,
                receipt: original,
            }) => {
                assert_eq!(repeated, source);
                assert_eq!(original, receipt);
            }
            _ => return Err("handler deadline lost its original replay".into()),
        }
        restored.push(address);
    }
    drop(store);
    drop(provider);
    let reopened = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let store = Store::new(reopened, root()?);
    for address in restored {
        let snapshot = store.load(&address).await?;
        let state = snapshot.state().ok_or("missing reopened deadline state")?;
        assert_eq!(state.cursor("inbox"), Some(1));
        assert_eq!(state.checkpoint(), Some(&previous));
    }
    Ok(())
}
