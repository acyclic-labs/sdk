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
fn placement<P>(stream: &Arc<P>) -> Result<Placement<P, Objects>> {
    Ok(placed(stream)?.0)
}

/// The one placement over `stream`, and its object bucket.
fn placed<P>(stream: &Arc<P>) -> Result<(Placement<P, Objects>, Bucket)> {
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let objects = Arc::new(objects);
    let observed = Bucket(objects.clone(), bucket.clone());
    Ok((
        Placement {
            stream: stream.clone(),
            objects: LogicalObjectStore::new(objects, bucket),
            capabilities: EmbeddedCapabilities::MEMORY,
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
    assert_eq!(
        bound
            .create_root(&intent("root"), root(id(1), vec![private]))
            .await?,
        child
    );
    // The root's private volume is live: an input stages on it.
    bound
        .stage_text(&created, "message", &intent("m1"), "hello")
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
    // second fresh volume is granted Write, and how the scope is spoiled.
    let run = |issued: Authority,
               given: Authority,
               agent: Option<acyclic_harness_core::AgentId>,
               operations: &'static [VolumeOperation],
               shared_write: bool,
               spoil: Spoil| async move {
        let stream = Arc::new(Refusing::default());
        let issuer = AuthorityRoot::new("agents", [7; 32], issued.clone());
        let private = volume(PRIVATE_VOLUME, id(7))?;
        let shared = volume("shared", id(7))?;
        let mut grants = vec![INTERACTION_ROUTE.to_owned()];
        for operation in operations {
            grants.push(private.volume.capability(*operation, &mut None)?);
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
            Spoil::Nothing
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
            run(issued, given, agent, operations, shared_write, spoil).await?,
            (false, 0, false),
            "{case}: refused before any stream or object IO"
        );
    }
    Ok(())
}
