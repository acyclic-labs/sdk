//! The admitted root producer: one placement builds both the agents and their
//! volumes, a root's fresh volumes and adoption are one commit prepared
//! without stream IO and held until it settles, and an admission that does
//! not cover it is refused before any IO.

use super::{Result, id, intent, wired::Refusing};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_fs::{EmbeddedCapabilities, LogicalObjectStore};
use acyclic_harness_core::{
    OperationId,
    agent::*,
    contract::{Capabilities, capability::INTERACTION_ROUTE},
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot, Scope},
    executor::Executor,
    filesystem::{
        PRIVATE_VOLUME, TurnAuthority, TurnExecutors, TurnLimits,
        hosting::{Bound, Placement, RootRequest, RootSubmission, Turns, VerifiedCaller},
    },
    model::ModelContent,
    resources::ProviderRef,
};
use acyclic_stream::{BoxProviderFuture, MemoryStream};
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

#[cfg(all(target_os = "linux", feature = "native-execution"))]
#[tokio::test]
async fn guest_admission_refuses_wrong_volume_provider_and_hosted_mapping_before_io() -> Result {
    use acyclic_fs::native_mount::GuestPosixPolicy;
    use acyclic_harness_core::filesystem::hosting::LocalGuestEnrollment;
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
        hosted.guest_session(enrollment(&reference)?, &work).await,
        Err(acyclic_harness_core::Error::Unsupported(_))
    ));
    let other = ProviderRef::new("other".into(), "filesystem".into(), "2".into(), &mut None)?;
    assert!(
        local
            .guest_session(enrollment(&other)?, &work)
            .await
            .is_err()
    );
    assert_eq!(stream.calls.load(Ordering::SeqCst), 0);
    assert_eq!(bucket.objects().await?, 0);
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    Ok(())
}

/// The volumes' object bucket, to observe what preparation writes.
struct Bucket(
    Arc<acyclic_objects::v1::MemoryObjects>,
    acyclic_objects::v1::wire::BucketRef,
);

impl Bucket {
    /// How many objects the bucket holds.
    async fn objects(&self) -> Result<usize> {
        use acyclic_objects::v1::ObjectsProvider as _;
        Ok(self
            .0
            .list(acyclic_objects::v1::wire::ListObjectsRequest {
                bucket: Some(self.1.clone()),
                ..Default::default()
            })
            .await?
            .entries
            .len())
    }
}

/// The one placement over `stream`.
fn placement<P: 'static>(stream: &Arc<P>) -> Result<Placement<P, Objects>> {
    Ok(placed(stream)?.0)
}

/// The one placement over `stream`, and its object bucket.
fn placed<P: 'static>(stream: &Arc<P>) -> Result<(Placement<P, Objects>, Bucket)> {
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let objects = Arc::new(objects);
    let observed = Bucket(objects.clone(), bucket.clone());
    Ok((
        Placement {
            filesystem: acyclic_fs::Fs::new(
                acyclic_fs::StreamAuthorityStore::new(stream.clone()),
                LogicalObjectStore::new(objects, bucket),
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
            .create_root(&intent("root"), root(id(1), vec![private]))
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

/// How an otherwise covering admission is spoiled after issue.
#[derive(Clone, Copy, Debug)]
enum Spoil {
    Nothing,
    Expired,
    Revoked,
}

#[tokio::test]
async fn an_admission_that_does_not_cover_the_root_is_refused_before_any_io() -> Result {
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "workload".into(),
    };
    let other = Authority {
        kind: AggregateKind::Conversation,
        id: "other".into(),
    };
    let stranger = acyclic_harness_core::AgentId::from_bytes(id(8).to_bytes());
    // Each case: the audience the admission is issued for, the one the host
    // verified, its bound agent, the private volume's operations, whether a
    // second fresh volume is granted Write, how the scope is spoiled, and
    // whether the admission is instead one structured grant over the private
    // volume alone.
    let run = |issued: Authority,
               given: Authority,
               agent: Option<acyclic_harness_core::AgentId>,
               operations: &'static [VolumeOperation],
               shared_write: bool,
               spoil: Spoil,
               structured: bool| async move {
        let stream = Arc::new(Refusing::default());
        let issuer = AuthorityRoot::new("agents", [7; 32], issued.clone());
        let private = volume(PRIVATE_VOLUME, id(7))?;
        let shared = volume("shared", id(7))?;
        let mut grants = vec![INTERACTION_ROUTE.to_owned()];
        for operation in operations {
            grants.push(private.volume.capability(*operation, &mut None)?);
        }
        if structured {
            grants = vec![
                INTERACTION_ROUTE.to_owned(),
                acyclic_fs::access::Capability::whole_volume(
                    Some(private.volume.id().to_owned()),
                    &acyclic_fs::access::Action::ALL,
                )
                .to_scope_label()?,
            ];
        }
        if shared_write {
            grants.push(
                shared
                    .volume
                    .capability(VolumeOperation::Write, &mut None)?,
            );
        }
        let mut scope: Scope =
            issuer.bootstrap_scope("admitted", &Capabilities::new(grants), agent, &mut None)?;
        match spoil {
            Spoil::Nothing => {}
            // A live deadline narrowed offline, then allowed to elapse.
            Spoil::Expired => {
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
                let deadline = now.as_secs() + 1;
                scope = scope.with_workload_expiry(
                    deadline,
                    &issuer.public_workload_trust()?,
                    &issued,
                    now.as_secs(),
                )?;
                let deadline = std::time::Duration::from_secs(deadline);
                std::thread::sleep(deadline.saturating_sub(now));
            }
            Spoil::Revoked => issuer.revoke_workload(&scope)?,
        }
        let (placement, bucket) = placed(&stream)?;
        let bound = Bound::admitted(
            issuer,
            VerifiedCaller {
                audience: given,
                scope,
            },
            placement,
            turns()?,
        )?;
        let prepared = bound
            .prepare_root(&intent("root"), root(id(7), vec![private, shared]))
            .await;
        // Stream IO, and whether objects were written: only a covering
        // admission writes its fresh volumes' initial objects.
        let written = bucket.objects().await? > 0;
        Ok::<_, Box<dyn std::error::Error>>((
            prepared.is_ok(),
            stream.calls.load(Ordering::SeqCst),
            written,
        ))
    };
    let both: &'static [VolumeOperation] = &[VolumeOperation::Read, VolumeOperation::Write];
    // Control: a covering admission prepares its root, and preparing is pure.
    assert_eq!(
        run(
            audience.clone(),
            audience.clone(),
            None,
            both,
            true,
            Spoil::Nothing,
            false
        )
        .await?,
        (true, 0, true),
        "a covering admission prepares without stream IO"
    );
    for (case, issued, given, agent, operations, shared_write, spoil) in [
        (
            "read-only fresh private",
            audience.clone(),
            audience.clone(),
            None,
            &[VolumeOperation::Read][..],
            true,
            Spoil::Nothing,
        ),
        (
            "read-only fresh shared",
            audience.clone(),
            audience.clone(),
            None,
            both,
            false,
            Spoil::Nothing,
        ),
        (
            "audience mismatch",
            other.clone(),
            audience.clone(),
            None,
            both,
            true,
            Spoil::Nothing,
        ),
        (
            "agent mismatch",
            audience.clone(),
            audience.clone(),
            Some(stranger),
            both,
            true,
            Spoil::Nothing,
        ),
        (
            "expired",
            audience.clone(),
            audience.clone(),
            None,
            both,
            true,
            Spoil::Expired,
        ),
        (
            "revoked",
            audience.clone(),
            audience.clone(),
            None,
            both,
            true,
            Spoil::Revoked,
        ),
    ] {
        assert_eq!(
            run(issued, given, agent, operations, shared_write, spoil, false).await?,
            (false, 0, false),
            "{case}: refused before any stream or object IO"
        );
    }
    // A structured grant over the private volume alone admits the agent but
    // not a second fresh volume it does not cover.
    assert_eq!(
        run(
            audience.clone(),
            audience,
            None,
            &[],
            false,
            Spoil::Nothing,
            true
        )
        .await?,
        (false, 0, false),
        "a structured grant not covering a fresh volume is refused before any IO"
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
