//! A parent turn creates two children whose activation appends coalesce.
use super::*;
use crate::guest::{Composer, Composition, Delivery, GuestError, Participant};
use acyclic_native_runtime::testing::{
    Schedule, fill_seeded, seeded_runtime,
    sweep::{seeds, sweep},
};

struct Children {
    parent: ActorReference,
    package: Package,
    first: Vec<CommitEffect>,
    second: Vec<CommitEffect>,
    settled: Arc<AtomicUsize>,
}

impl Handler for Children {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            let parent = delivery.actor == self.parent;
            let mut transition = self
                .package
                .on_record(delivery, checkpoint, cancellation)
                .await?;
            if parent {
                transition.effects.extend(self.first.clone());
                transition.participant = Some(Participant {
                    kind: "children".into(),
                    plan: Bytes::new(),
                });
            }
            Ok(transition)
        })
    }
}

impl Composer for Children {
    fn open(
        &self,
        actor: &ActorReference,
        _: Option<&Checkpoint>,
        _: Option<&acyclic_stream::Record>,
        _: &Checkpoint,
        participant: &Participant,
    ) -> Result<Box<dyn Composition>, GuestError> {
        assert_eq!(actor, &self.parent);
        assert_eq!(participant.kind, "children");
        Ok(Box::new(Spawn {
            effects: self.second.clone(),
            settled: self.settled.clone(),
        }))
    }
}

struct Spawn {
    effects: Vec<CommitEffect>,
    settled: Arc<AtomicUsize>,
}

impl Composition for Spawn {
    fn seal<'a>(&'a mut self, request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        Box::pin(async move {
            let deadline = request.deadline_unix_millis;
            let mut publication = PreparedPublish::foreign(request.idempotency_key);
            for effect in request.effects.into_iter().chain(self.effects.clone()) {
                publication
                    .join(effect)
                    .map_err(|error| GuestError::Handler(error.to_string()))?;
            }
            let mut request = publication
                .into_request()
                .map_err(|error| GuestError::Handler(error.to_string()))?;
            request.deadline_unix_millis = deadline;
            Ok(request)
        })
    }

    fn settle<'a>(
        self: Box<Self>,
        outcome: Result<&'a acyclic_stream::CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()> {
        Box::pin(async move {
            assert!(matches!(
                outcome,
                Ok(acyclic_stream::CommitOutcome::Committed(_))
            ));
            self.settled.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

fn child_effects(
    store: &Store<MemoryStream>,
    child: &ActorReference,
) -> Result<Vec<CommitEffect>, Box<dyn std::error::Error>> {
    let inbox = StreamPath::new(format!("agents/{}", child.id()))?;
    Ok(vec![
        store.create(
            None,
            child,
            crate::local::Creation {
                subscriptions: BTreeMap::from([(
                    "inbox".into(),
                    Subscription {
                        path: inbox.clone(),
                        paused: false,
                        cursor: 0,
                    },
                )]),
                checkpoint: Some(ledger(&format!("out/{}", child.id()))?),
                escrow: None,
            },
        )?,
        store.activation(child)?,
        CommitEffect::hard(
            vec![CommitCondition::Absent {
                path: inbox.clone(),
            }],
            vec![CommitMutation::Append {
                path: inbox,
                records: vec![Bytes::from_static(b"emit").into()],
            }],
        ),
    ])
}

async fn history() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(provider.clone(), root()?);
    let mut entropy = [0; 8];
    assert!(fill_seeded(&mut entropy));
    let prefix = u64::from_le_bytes(entropy);
    let mut shards = BTreeMap::new();
    let mut candidate = 0u64;
    // This is a finite identity search, not a readiness check. Pigeonhole
    // guarantees a collision after at most shard_count + 1 candidates.
    let children = loop {
        let child = actor(&format!("child-{prefix}-{candidate}"))?;
        if let Some(first) = shards.insert(store.scheduling_shard(&child), child.clone()) {
            break [first, child];
        }
        candidate += 1;
    };
    assert_ne!(children[0], children[1]);
    assert_eq!(
        store.scheduling_shard(&children[0]),
        store.scheduling_shard(&children[1])
    );
    let parent = spawn_with(&store, "composing-parent", &[b"emit"]).await?;
    let settled = Arc::new(AtomicUsize::new(0));
    let handler = Arc::new(Children {
        parent: parent.clone(),
        package: Package::new()?,
        first: child_effects(&store, &children[0])?,
        second: child_effects(&store, &children[1])?,
        settled: settled.clone(),
    });
    let store = store.with_composer(handler.clone());
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store.clone(),
        handler,
        Arc::new(SystemClock),
    );
    runtime.host(parent.clone())?;
    runtime.drain_actor(&parent).await?;
    assert_eq!(settled.load(Ordering::SeqCst), 1);
    let parent_cut = store.load(&parent).await?;
    assert_eq!(
        parent_cut.state().and_then(|state| state.cursor("inbox")),
        Some(1)
    );
    for child in &children {
        let cut = store.load(child).await?;
        assert_eq!(
            cut.anchor, parent_cut.anchor,
            "child creation escaped the parent's commit"
        );
        assert_eq!(cut.state().and_then(|state| state.cursor("inbox")), Some(0));
    }
    // The real dispatcher must find both children through their shared
    // shard's activation records; nothing hosts or drains them directly.
    {
        let mut serving = Box::pin(runtime.run(|_, _| {}));
        for child in &children {
            // Simulated time: a child nothing wakes fails here, not by hanging.
            tokio::time::timeout(
                std::time::Duration::from_secs(3600),
                observe_while_running(
                    serving.as_mut(),
                    state_after(&store, child, |state| state.cursor("inbox") == Some(1)),
                ),
            )
            .await
            .map_err(|_| format!("composed child {} never woke", child.id()))??;
        }
    }
    for child in &children {
        assert_eq!(
            records(provider.as_ref(), &format!("out/{}", child.id())).await?,
            vec![Bytes::from_static(b"r0")]
        );
    }
    // Re-draining the parent must not rerun its composition or recreate children.
    runtime.drain_actor(&parent).await?;
    assert_eq!(settled.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn seeded_children_share_one_activation_shard() -> TestResult {
    sweep("actors-composition", seeds(0..4), |seed| {
        seeded_runtime(seed).map_err(|error| error.to_string())?.block_on(async {
            tokio::time::pause();
            let (schedule, mut boundaries) = Schedule::seeded(seed);
            let turn = schedule.scope(history());
            tokio::pin!(turn);
            loop {
                tokio::select! {
                    result = &mut turn => return result.map_err(|error| error.to_string()),
                    boundary = boundaries.recv() => boundary.ok_or("boundary channel closed")?.release(),
                }
            }
        })
    })?;
    Ok(())
}
