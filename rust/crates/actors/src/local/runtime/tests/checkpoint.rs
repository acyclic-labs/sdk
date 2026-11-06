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

/// A logical checkpoint crosses physical record boundaries without losing its
/// original invocation result, consumed cursor or exact bytes after reopening.
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
    let checkpoint = Checkpoint {
        codec: CodecIdentity {
            name: "checkpoint-boundary".into(),
            version: "1".into(),
        },
        bytes: vec![0x35; acyclic_stream::MAX_RECORD_BYTES + 1].into(),
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
        Bytes::from_static(b"persist"),
        Bytes::from_static(b"original response"),
    )?;
    let ticket = store.enqueue_invoke(&invocation).await?;
    let result = runtime.drain_actor(&address).await;
    assert!(
        result.is_ok(),
        "unbounded logical checkpoint must publish independently of record size: {result:?}"
    );
    let completed = store.wait_invocation(ticket).await?;
    assert_eq!(completed.response().as_ref(), invocation.request().as_ref());
    assert_eq!(
        completed.receipt().state().commit_id,
        completed.receipt().terminal().commit_id
    );
    drop(completed);
    drop(runtime);
    drop(store);
    drop(provider);
    let reopened = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let restored = Store::new(reopened, root()?).load(&address).await?;
    let state = restored.state().ok_or("missing reopened state")?;
    assert_eq!(state.cursor("inbox"), Some(1));
    assert_eq!(state.checkpoint(), Some(&checkpoint));
    Ok(())
}
