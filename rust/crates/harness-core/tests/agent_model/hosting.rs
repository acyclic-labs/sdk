//! The admitted root producer: one placement builds both the agents and their
//! volumes, and a root's fresh volumes and adoption are one commit prepared
//! without stream IO and held until it settles. Refusal vectors run for
//! every host in the agent-host suite.

use super::{Result, id, intent};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_fs::{EmbeddedCapabilities, LogicalObjectStore};
use acyclic_harness_core::{
    OperationId,
    agent::*,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::Executor,
    filesystem::{
        PRIVATE_VOLUME, TurnAuthority, TurnExecutors, TurnLimits,
        attestation::{Attempt, Attestation, AttestedProvider, Attestor},
        hosting::{Bound, Placement, RootRequest, RootSubmission, Turns},
    },
    model::ModelContent,
    resources::ProviderRef,
};
use acyclic_stream::{BoxProviderFuture, MemoryStream, StreamError, StreamProvider};
use std::sync::{Arc, atomic::Ordering};

/// Never builds an executor: these tests only create roots.
struct Never;

impl TurnExecutors for Never {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        _: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Arc<dyn Executor>>> {
        Box::pin(async {
            Err(acyclic_harness_core::Error::Unsupported(
                "no turns in this test".into(),
            ))
        })
    }
}

type Objects = LogicalObjectStore<acyclic_objects::v1::MemoryObjects>;

fn provider() -> Result<ProviderRef> {
    Ok(ProviderRef::new(
        "agents".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?)
}

/// The one placement over `stream`.
fn placement<P>(stream: &Arc<P>) -> Result<Placement<P, Objects>> {
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    Ok(Placement {
        stream: stream.clone(),
        objects: LogicalObjectStore::new(Arc::new(objects), bucket),
        capabilities: EmbeddedCapabilities::MEMORY,
        provider: provider()?,
        within: ActorReference::new(b"account", b"agents", b"owner")?,
        attestor: None,
    })
}

fn turns() -> Result<Turns> {
    Ok(Turns {
        executors: Arc::new(Never),
        limits: TurnLimits {
            maximum_bytes: 64 * 1024,
            max_steps: 4,
            history_turns: 8,
        },
        actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        clock: Arc::new(SystemClock),
    })
}

fn volume(name: &str, agent: AgentId) -> Result<VolumeBinding> {
    Ok(VolumeBinding {
        name: name.into(),
        volume: VolumeRef::new(
            provider()?,
            format!("agent-{name}"),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(agent.to_bytes())),
            &mut None,
        )?,
    })
}

fn root(agent: AgentId, fresh: Vec<VolumeBinding>) -> RootRequest {
    RootRequest {
        agent,
        fresh,
        retained: Vec::new(),
        brief: None,
        policy: None,
    }
}

#[tokio::test]
async fn create_root_is_its_prepared_commit_and_an_unknown_resubmits_it_exactly() -> Result {
    let stream = Arc::new(MemoryStream::default());
    let bound = Bound::local_root(
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        placement(&stream)?,
        turns()?,
    )?;
    let private = volume(PRIVATE_VOLUME, id(1))?;
    let first = bound
        .prepare_root(&intent("root"), root(id(1), vec![private.clone()]))
        .await?;
    let again = bound
        .prepare_root(&intent("root"), root(id(1), vec![private.clone()]))
        .await?;
    assert_eq!(first.request(), again.request(), "preparation is exact");
    let child = first.child().clone();
    let RootSubmission::Created(created) = bound.submit_root(first).await? else {
        return Err("the root's submission was uncertain".into());
    };
    assert_eq!(created, child);
    assert_eq!(
        bound.agents().state(&created).await?.volumes,
        vec![private.clone()]
    );
    // The Unknown path: the identical prepared commit resubmits as the same
    // creation, and creating again under the intent replays it.
    assert!(matches!(
        bound.submit_root(again).await?,
        RootSubmission::Created(replayed) if replayed == child
    ));
    assert!(matches!(
        bound
            .create_root(&intent("root"), root(id(1), vec![private.clone()]))
            .await?,
        RootSubmission::Created(replayed) if replayed == child
    ));
    let (alice, bob) = (
        Sender::User {
            principal: "alice".into(),
        },
        Sender::User {
            principal: "bob".into(),
        },
    );
    // The root's private volume is live: an input stages on it.
    bound
        .stage_text(&created, "message", &alice, &intent("m1"), "hello")
        .await?;
    // Another sender's same intent with different text stages its own input.
    bound
        .stage_text(&created, "message", &bob, &intent("m1"), "hi")
        .await?;
    Ok(())
}

/// A provider that admits a write only with valid evidence: it counts
/// publications, records each presented attestation, and can lose the
/// first commit's acknowledgement before applying it.
#[derive(Default)]
struct Evidence {
    inner: MemoryStream,
    valid: bool,
    unknown_first: std::sync::atomic::AtomicBool,
    presented: std::sync::Mutex<Vec<Attestation>>,
    published: std::sync::atomic::AtomicUsize,
}

impl AttestedProvider for Evidence {
    fn bind(
        &self,
        attestation: &Attestation,
    ) -> std::result::Result<Option<Arc<Self>>, StreamError> {
        self.presented
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .push(attestation.clone());
        if !self.valid || attestation.0.len() != 32 {
            return Err(StreamError::AccessDenied);
        }
        Ok(None)
    }
}

#[async_trait::async_trait]
impl StreamProvider for Evidence {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.inner.operation_claims()
    }
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<u64, StreamError> {
        self.inner.tail(path).await
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> std::result::Result<acyclic_stream::AppendOutcome, StreamError> {
        self.published.fetch_add(1, Ordering::SeqCst);
        self.inner.append(request).await
    }
    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, StreamError> {
        self.published.fetch_add(1, Ordering::SeqCst);
        self.inner.fork(request).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
        filter: acyclic_stream::FollowFilter,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        self.inner.follow(path, from, filter).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, StreamError> {
        if self.unknown_first.swap(false, Ordering::SeqCst) {
            return Err(StreamError::Unavailable);
        }
        self.published.fetch_add(1, Ordering::SeqCst);
        self.inner.commit(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

/// Attests each digest with itself, recording every request for evidence.
#[derive(Default)]
struct Digests(std::sync::Mutex<Vec<[u8; 32]>>);

impl Attestor for Digests {
    fn attest<'a>(
        &'a self,
        attempt: Attempt<'a>,
    ) -> BoxProviderFuture<'a, std::result::Result<Attestation, StreamError>> {
        Box::pin(async move {
            self.0
                .lock()
                .map_err(|_| StreamError::Unavailable)?
                .push(attempt.digest);
            Ok(Attestation(attempt.digest.to_vec().into()))
        })
    }
}

fn attested(stream: &Arc<Evidence>, attestor: &Arc<Digests>) -> Result<Bound<Evidence, Objects>> {
    let mut placement = placement(stream)?;
    placement.attestor = Some(attestor.clone());
    Ok(Bound::local_root(
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        placement,
        turns()?,
    )?)
}

#[tokio::test]
async fn every_sealed_write_is_attested_and_an_unknown_retry_presents_the_same_evidence() -> Result
{
    let stream = Arc::new(Evidence {
        valid: true,
        unknown_first: true.into(),
        ..Evidence::default()
    });
    let attestor = Arc::new(Digests::default());
    let bound = attested(&stream, &attestor)?;
    let private = volume(PRIVATE_VOLUME, id(2))?;
    let prepared = bound
        .prepare_root(&intent("root"), root(id(2), vec![private]))
        .await?;
    // The acknowledgement is lost: the commit, still holding its objects,
    // comes back and resubmits exactly.
    let RootSubmission::Unknown(held) = bound.submit_root(prepared).await? else {
        return Err("the lost acknowledgement was not uncertain".into());
    };
    assert!(matches!(
        bound.submit_root(held).await?,
        RootSubmission::Created(created) if created == AgentPath::root(&id(2))
    ));
    let digests = attestor.0.lock().map_err(|_| "poisoned")?.clone();
    let presented = stream.presented.lock().map_err(|_| "poisoned")?.clone();
    // Every write presented exactly the evidence attested for its request.
    assert_eq!(presented.len(), digests.len(), "one attestation per write");
    for attestation in &presented {
        assert!(
            digests
                .iter()
                .any(|digest| digest.as_slice() == attestation.0.as_ref())
        );
    }
    // The root's lost attempt and its exact retry are the same sealed
    // request, so they present the identical attestation.
    assert!(presented.len() >= 2);
    assert_eq!(
        presented[0], presented[1],
        "the uncertain retry presents the identical attestation"
    );
    Ok(())
}

#[tokio::test]
async fn a_write_without_valid_evidence_publishes_nothing() -> Result {
    let stream = Arc::new(Evidence::default());
    let attestor = Arc::new(Digests::default());
    let bound = attested(&stream, &attestor)?;
    let private = volume(PRIVATE_VOLUME, id(3))?;
    assert!(!matches!(
        bound
            .create_root(&intent("root"), root(id(3), vec![private]))
            .await,
        Ok(RootSubmission::Created(_))
    ));
    assert_eq!(stream.published.load(Ordering::SeqCst), 0);
    assert_eq!(
        stream.presented.lock().map_err(|_| "poisoned")?.len(),
        1,
        "the root's one request was presented once"
    );
    Ok(())
}

#[tokio::test]
async fn trees_sharing_a_stream_each_claim_only_their_own_agents() -> Result {
    let stream = Arc::new(MemoryStream::default());
    let bind = |namespace: &[u8]| -> Result<Bound<MemoryStream, Objects>> {
        let mut placement = placement(&stream)?;
        placement.within = ActorReference::new(b"account", namespace, b"owner")?;
        Ok(Bound::local_root(
            AuthorityRoot::new(
                "agents",
                [7; 32],
                Authority {
                    kind: AggregateKind::Conversation,
                    id: "local".into(),
                },
            ),
            placement,
            turns()?,
        )?)
    };
    let create = async |bound: &Bound<MemoryStream, Objects>, agent: u8| -> Result<AgentPath> {
        let request = root(id(agent), vec![volume(PRIVATE_VOLUME, id(agent))?]);
        match bound
            .create_root(&intent(&format!("root-{agent}")), request)
            .await?
        {
            RootSubmission::Created(created) => Ok(created),
            RootSubmission::Unknown(_) => Err("the root's submission was uncertain".into()),
        }
    };
    // Two servers, one per tree, on one stream; each tree's later roots come
    // from another binding of the same tree, so its server discovers them.
    let (a, b) = (bind(b"tree-a")?, bind(b"tree-b")?);
    let (a2, b2) = (bind(b"tree-a")?, bind(b"tree-b")?);
    let root_b = create(&b, 2).await?;
    create(&a, 1).await?;
    let foreign = b.agents().actor(&root_b)?;
    assert!(a.agents().runtime().track(foreign).is_err());
    let runners: Vec<_> = [a.agents(), b.agents()]
        .into_iter()
        .map(|agents| {
            let agents = Arc::clone(agents);
            tokio::spawn(async move { agents.run(|_, _| {}).await })
        })
        .collect();
    // The activation log orders b's roots around a's: a reaching its second
    // root has passed b's first two, and b reaching its third has passed a's.
    create(&b2, 3).await?;
    create(&a2, 4).await?;
    create(&b2, 5).await?;
    let resident = |bound: &Bound<MemoryStream, Objects>| bound.agents().runtime().counts().0;
    for _ in 0..2_000 {
        if resident(&a) == 2 && resident(&b) == 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert_eq!((resident(&a), resident(&b)), (2, 3));
    for runner in runners {
        runner.abort();
    }
    Ok(())
}
