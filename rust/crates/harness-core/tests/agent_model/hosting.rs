//! The admitted root producer: a root's fresh volumes and adoption are one
//! commit prepared without IO, and an admission that does not cover it is
//! refused before any.

use super::{Result, id, intent, wired::Refusing};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_fs::{EmbeddedCapabilities, Fs, LogicalObjectStore, StreamAuthorityStore};
use acyclic_harness_core::{
    agent::*,
    contract::{Capabilities, capability::INTERACTION_ROUTE},
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::{
        FilesystemHost, PRIVATE_VOLUME,
        hosting::{Bound, Prepared, RootRequest, VerifiedCaller},
    },
    resources::ProviderRef,
};
use acyclic_stream::{MemoryStream, StreamProvider};
use std::sync::{Arc, atomic::Ordering};

/// Never runs a turn: these tests only create roots.
struct Idle;

impl TurnDriver for Idle {
    fn step<'a>(
        &'a self,
        _agent: &'a AgentState,
        _cursor: u64,
        _input: Option<TurnInput>,
    ) -> TurnFuture<'a> {
        Box::pin(async {
            Ok(TurnStep {
                checkpoint: None,
                outs: Vec::new(),
                wait: Wait::Input,
            })
        })
    }
}

type Objects = LogicalObjectStore<acyclic_objects::v1::MemoryObjects>;

/// Agents and volumes on the one stream `provider`, and the volumes' provider.
fn stores<P: StreamProvider>(
    provider: &Arc<P>,
) -> Result<(
    Arc<AgentHost<P>>,
    Arc<FilesystemHost<StreamAuthorityStore<P>, Objects>>,
    ProviderRef,
)> {
    let reference = ProviderRef::new("agents".into(), "filesystem".into(), "2".into(), &mut None)?;
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let volumes = FilesystemHost::new(
        Fs::new(
            StreamAuthorityStore::new(provider.clone()),
            LogicalObjectStore::new(Arc::new(objects), bucket),
            EmbeddedCapabilities::MEMORY,
        ),
        &reference,
        None,
    )?;
    let agents = Arc::new(AgentHost::new(
        provider.clone(),
        Idle,
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
    )?);
    Ok((agents, volumes, reference))
}

fn volume(reference: &ProviderRef, name: &str, agent: AgentId) -> Result<VolumeBinding> {
    Ok(VolumeBinding {
        name: name.into(),
        volume: VolumeRef::new(
            reference.clone(),
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
    let provider = Arc::new(MemoryStream::default());
    let (agents, volumes, reference) = stores(&provider)?;
    let bound = Bound::local_root(
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        agents.clone(),
        volumes,
    );
    let private = volume(&reference, PRIVATE_VOLUME, id(1))?;
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
    assert_eq!(agents.state(&created).await?.volumes, vec![private]);
    // The Unknown path: the exact prepared bytes resubmit as the same creation.
    let Prepared { request, .. } = first;
    assert!(matches!(
        submit(&*provider, request).await?,
        Submitted::Committed(_)
    ));
    Ok(())
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
    // verified, its bound agent, the private volume's operations, and whether
    // a second fresh volume is granted Write.
    let run = |issued: Authority,
               given: Authority,
               agent: Option<acyclic_harness_core::AgentId>,
               operations: &'static [VolumeOperation],
               shared_write: bool| async move {
        let stream = Arc::new(Refusing::default());
        let (agents, volumes, reference) = stores(&stream)?;
        let issuer = AuthorityRoot::new("agents", [7; 32], issued);
        let private = volume(&reference, PRIVATE_VOLUME, id(7))?;
        let shared = volume(&reference, "shared", id(7))?;
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
        let scope =
            issuer.bootstrap_scope("admitted", &Capabilities::new(grants), agent, &mut None)?;
        let bound = Bound::admitted(
            issuer,
            VerifiedCaller {
                audience: given,
                scope,
            },
            agents,
            volumes,
        );
        let prepared = bound
            .prepare_root(&intent("root"), root(id(7), vec![private, shared]))
            .await;
        Ok::<_, Box<dyn std::error::Error>>((prepared.is_ok(), stream.calls.load(Ordering::SeqCst)))
    };
    let both: &'static [VolumeOperation] = &[VolumeOperation::Read, VolumeOperation::Write];
    // Control: a covering admission prepares its root, and preparing is pure.
    assert_eq!(
        run(audience.clone(), audience.clone(), None, both, true).await?,
        (true, 0),
        "a covering admission prepares without IO"
    );
    for (case, issued, given, agent, operations, shared_write) in [
        (
            "read-only fresh private",
            audience.clone(),
            audience.clone(),
            None,
            &[VolumeOperation::Read][..],
            true,
        ),
        (
            "read-only fresh shared",
            audience.clone(),
            audience.clone(),
            None,
            both,
            false,
        ),
        (
            "audience mismatch",
            other.clone(),
            audience.clone(),
            None,
            both,
            true,
        ),
        (
            "agent mismatch",
            audience.clone(),
            audience.clone(),
            Some(stranger),
            both,
            true,
        ),
    ] {
        assert_eq!(
            run(issued, given, agent, operations, shared_write).await?,
            (false, 0),
            "{case}: refused before any stream IO"
        );
    }
    Ok(())
}
