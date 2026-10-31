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
        PRIVATE_VOLUME, TurnAdmission, TurnAuthority, TurnExecutors, TurnLimits,
        attestation::{Attempt, Attestation, Attested, AttestedProvider, Attestor},
        hosting::{Bound, Placement, RootRequest, RootSubmission, Turns},
    },
    model::ModelContent,
    resources::ProviderRef,
};
use acyclic_stream::StreamProviderExt as _;
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

type Objects = LogicalObjectStore<acyclic_objects::MemoryObjects>;

fn provider() -> Result<ProviderRef> {
    Ok(ProviderRef::new(
        "agents".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?)
}

#[cfg(all(target_os = "linux", feature = "native-execution"))]
#[tokio::test]
async fn guest_admission_refuses_wrong_volume_provider_and_hosted_mapping_before_io() -> Result {
    use super::wired::Refusing;
    use acyclic_fs::native_mount::GuestPosixPolicy;
    use acyclic_harness_core::{
        contract::Capabilities,
        filesystem::hosting::{LocalGuestEnrollment, VerifiedCaller},
    };
    acyclic_native_runtime::initialize_native_io_services()?;
    acyclic_fs::initialize_native_mount_services()?;
    let stream = Arc::new(Refusing::default());
    let (placed, bucket) = placed(&stream)?;
    let reference = placed.provider.clone();
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "guest".into(),
    };
    let issuer = AuthorityRoot::new("agents", [7; 32], audience.clone());
    let scope = issuer.bootstrap_scope("guest", &Capabilities::default(), None, &mut None)?;
    let hosted = Bound::admitted(
        issuer.clone(),
        VerifiedCaller { audience, scope },
        placed,
        turns()?,
    )?;
    let local = Bound::local_root(issuer, placement(&stream)?, turns()?)?;
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::BOUNDARY_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, _)| cause)?;
    let enrollment = |provider: &ProviderRef| -> Result<LocalGuestEnrollment> {
        Ok(LocalGuestEnrollment {
            volume: VolumeRef::new(
                provider.clone(),
                "agent-guest".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(id(7).to_bytes())),
                &mut None,
            )?,
            posix: GuestPosixPolicy {
                identities: vec![(0, 0)],
                metadata: true,
            },
            writable: true,
        })
    };
    assert!(matches!(
        {
            let selected = enrollment(&reference)?;
            hosted
                .guest_session(
                    selected.volume,
                    selected.posix,
                    selected.writable,
                    None,
                    &work,
                )
                .await
        },
        Err(acyclic_harness_core::Error::Unsupported(_))
    ));
    let other = ProviderRef::new("other".into(), "filesystem".into(), "2".into(), &mut None)?;
    assert!(
        local
            .guest_session(
                enrollment(&other)?.volume,
                enrollment(&other)?.posix,
                true,
                None,
                &work
            )
            .await
            .is_err()
    );
    assert_eq!(stream.calls.load(Ordering::SeqCst), 0);
    assert_eq!(bucket.objects().await?, 0);
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    Ok(())
}

/// The volumes' blobs, to observe what preparation writes.
struct Bucket(Arc<acyclic_objects::MemoryObjects>);

impl Bucket {
    /// How many blobs the account holds.
    async fn objects(&self) -> Result<usize> {
        use acyclic_objects::Blobs as _;
        Ok(self.0.list(None, 1).await?.digests.len())
    }
}

/// The one placement over `stream`.
fn placement<P: 'static>(stream: &Arc<P>) -> Result<Placement<P, Objects>> {
    Ok(placed(stream)?.0)
}

/// The one placement over `stream`, and its blobs.
fn placed<P: 'static>(stream: &Arc<P>) -> Result<(Placement<P, Objects>, Bucket)> {
    let objects = acyclic_objects::MemoryObjects::memory();
    let objects = Arc::new(objects);
    let observed = Bucket(objects.clone());
    Ok((
        Placement {
            filesystem: acyclic_fs::Fs::new(
                acyclic_fs::StreamAuthorityStore::new(stream.clone()),
                LogicalObjectStore::new(objects),
                EmbeddedCapabilities::MEMORY,
            ),
            provider: provider()?,
            within: ActorReference::new(b"account", b"agents", b"owner")?,
        },
        observed,
    ))
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
        approval_policy: Default::default(),
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
async fn create_root_preparation_and_intent_replay_are_exact() -> Result {
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
    // The identical prepared commit and the original intent both replay the
    // same creation.
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
    let staged = bound
        .stage_text(
            &created,
            "message",
            &alice,
            &intent("m1"),
            ("/command", "hello"),
        )
        .await?;
    assert_eq!(
        bound
            .staged_text(&created, "message", &alice, &intent("m1"), "/command")
            .await?,
        Some(staged)
    );
    // Equal expanded bytes do not make a changed original request a replay.
    assert!(matches!(
        bound
            .staged_text(&created, "message", &alice, &intent("m1"), "/changed")
            .await,
        Err(acyclic_harness_core::agent::Error::Conflict(_))
    ));
    assert!(
        bound
            .stage_text(
                &created,
                "message",
                &alice,
                &intent("m1"),
                ("/changed", "hello")
            )
            .await
            .is_err()
    );
    // Another sender's same intent with different text stages its own input.
    bound
        .stage_text(&created, "message", &bob, &intent("m1"), ("hi", "hi"))
        .await?;
    Ok(())
}

/// A provider that admits a write only with valid evidence: it counts
/// publications, records each presented attestation, and can lose the
/// first commit's acknowledgement after applying it.
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
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        acyclic_stream::ProviderContext::new(self.inner.context().operation_claims())
    }
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }

    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
        self.inner.heads(request).await
    }
    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> acyclic_stream::BoxProviderFuture<
        '_,
        std::result::Result<acyclic_stream::WriteOutcome, acyclic_stream::StreamError>,
    > {
        match request {
            acyclic_stream::WriteRequest::Append(request) => {
                Box::pin(futures::TryFutureExt::map_ok(
                    async move {
                        self.published.fetch_add(1, Ordering::SeqCst);
                        self.inner.append(request).await
                    },
                    acyclic_stream::WriteOutcome::Append,
                ))
            }
            acyclic_stream::WriteRequest::Fork(request) => Box::pin(futures::TryFutureExt::map_ok(
                async move {
                    self.published.fetch_add(1, Ordering::SeqCst);
                    self.inner.fork(request).await
                },
                acyclic_stream::WriteOutcome::Fork,
            )),
            acyclic_stream::WriteRequest::Commit(request) => {
                Box::pin(futures::TryFutureExt::map_ok(
                    async move {
                        self.published.fetch_add(1, Ordering::SeqCst);
                        let outcome = self.inner.commit(request).await?;
                        if self.unknown_first.swap(false, Ordering::SeqCst) {
                            return Err(StreamError::Unavailable);
                        }
                        Ok(outcome)
                    },
                    acyclic_stream::WriteOutcome::Commit,
                ))
            }
        }
    }

    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> std::result::Result<acyclic_stream::FollowStream, StreamError> {
        self.inner.follow(request).await
    }

    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_stream::NativeStreamProvider for Evidence {}

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

/// A self-host whose provider requires evidence composes its placement stream
/// as [`Attested`], under the same admission its binding acts with.
fn attested(
    stream: &Arc<Evidence>,
    attestor: &Arc<Digests>,
) -> Result<Bound<Attested<Evidence>, Objects>> {
    let stream = Arc::new(Attested::new(
        stream.clone(),
        Some(attestor.clone() as Arc<dyn Attestor>),
        TurnAdmission::LocalRoot,
    ));
    Ok(Bound::local_root(
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
        .prepare_root(&intent("root"), root(id(2), vec![private.clone()]))
        .await?;
    let request = prepared.request().clone();
    // The original provider applies the whole sealed commit before losing
    // its reply. The uncertain owner comes back with the identical request.
    let RootSubmission::Unknown(held) = bound.submit_root(prepared).await? else {
        return Err("the lost acknowledgement was not uncertain".into());
    };
    assert_eq!(held.request(), &request);
    let original = stream
        .inner
        .inspect_idempotency(request.idempotency_key)
        .await?
        .ok_or("the unknown result must already be committed")?;
    let acyclic_stream::IdempotencyOutcome::Commit(envelope) = &original.outcome else {
        return Err("the original result is not the root commit".into());
    };
    assert_eq!(
        stream.inner.read_commit(envelope.commit_id).await?,
        *envelope
    );
    assert_eq!(
        bound.agents().state(held.child()).await?.volumes,
        vec![private],
        "the root and its selected volume exist before retry"
    );
    let paths = request
        .mutations
        .iter()
        .map(|mutation| match mutation {
            acyclic_stream::CommitMutation::Append { path, .. } => path.clone(),
            acyclic_stream::CommitMutation::Fork { destination, .. } => destination.clone(),
        })
        .collect::<acyclic_stream::HeadSet<_>>();
    let heads = acyclic_stream::HeadsRequest {
        limit: u32::try_from(paths.len())?,
        paths,
        retained: Default::default(),
        durable_through: None,
        projection: Default::default(),
        children: None,
        at_least: Default::default(),
    };
    let before = stream.inner.heads(heads.clone()).await?;
    assert!(before.heads.iter().all(Option::is_some));
    assert!(matches!(
        bound.submit_root(held).await?,
        RootSubmission::Created(created) if created == AgentPath::root(&id(2))
    ));
    assert_eq!(
        stream
            .inner
            .inspect_idempotency(request.idempotency_key)
            .await?,
        Some(original),
        "retry retains the original terminal envelope"
    );
    assert_eq!(
        stream.inner.heads(heads).await?.heads,
        before.heads,
        "retry appends no second volume, adoption, actor state or activation"
    );
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
    assert_eq!(presented.len(), 2);
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
