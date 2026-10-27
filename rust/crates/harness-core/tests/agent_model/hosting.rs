//! The admitted root producer: one placement builds both the agents and their
//! volumes, a root's fresh volumes and adoption are one commit prepared
//! without IO, and an admission that does not cover it is refused before any.

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
        hosting::{Bound, Placement, Prepared, RootRequest, Turns, VerifiedCaller},
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

/// The one placement over `stream`.
fn placement<P>(stream: &Arc<P>) -> Result<Placement<P, Objects>> {
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    Ok(Placement {
        stream: stream.clone(),
        objects: LogicalObjectStore::new(Arc::new(objects), bucket),
        capabilities: EmbeddedCapabilities::MEMORY,
        provider: provider()?,
        within: ActorReference::new(b"account", b"agents", b"owner")?,
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
    assert_eq!(
        first.request, again.request,
        "preparation is pure and exact"
    );
    let created = bound
        .create_root(&intent("root"), root(id(1), vec![private.clone()]))
        .await?;
    assert_eq!(created, first.child);
    assert_eq!(bound.agents().state(&created).await?.volumes, vec![private]);
    // The Unknown path: the exact prepared bytes resubmit as the same creation.
    let Prepared { request, .. } = first;
    assert!(matches!(
        submit(&*stream, request).await?,
        Submitted::Committed(_)
    ));
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
        let bound = Bound::admitted(
            issuer,
            VerifiedCaller {
                audience: given,
                scope,
            },
            placement(&stream)?,
            turns()?,
        )?;
        let prepared = bound
            .prepare_root(&intent("root"), root(id(7), vec![private, shared]))
            .await;
        Ok::<_, Box<dyn std::error::Error>>((prepared.is_ok(), stream.calls.load(Ordering::SeqCst)))
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
        (true, 0),
        "a covering admission prepares without IO"
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
            (false, 0),
            "{case}: refused before any stream IO"
        );
    }
    Ok(())
}
