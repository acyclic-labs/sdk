//! Admitted root preparation: one placement builds the agents and their
//! volumes, and a root's fresh volumes and adoption are one commit prepared
//! without stream IO. An admission that does not cover the root is refused
//! before any provider read or write.
use crate::conformance::{Backend, Result, observe::Observed, vectors};
use acyclic_actors::{addressing::ActorReference, local::SystemClock};
use acyclic_fs::{EmbeddedCapabilities, LogicalObjectStore};
use acyclic_harness_core::{
    Capabilities, OperationId,
    agent::*,
    contract::capability::INTERACTION_ROUTE,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::Executor,
    filesystem::{
        PRIVATE_VOLUME, TurnAuthority, TurnExecutors, TurnLimits,
        hosting::{Bound, Placement, RootRequest, RootSubmission, Turns, VerifiedCaller},
    },
    model::ModelContent,
    resources::ProviderRef,
};
use acyclic_stream::{
    BoxProviderFuture, UnixMillisClock,
    runtime::{Executor as _, PlatformExecutor},
};
use std::sync::{Arc, atomic::Ordering};

/// Root preparation never runs a turn.
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
            Err(acyclic_harness_core::Error::Invalid(
                "conformance root preparation ran a turn".into(),
            ))
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    Covering,
    ReadOnlyPrivate,
    ReadOnlyShared,
    WrongAudience,
    OtherAgent,
    Expired,
    Revoked,
    /// One structured grant over the private volume alone, which admits the
    /// agent but not the second fresh volume.
    StructuredPrivateOnly,
}

pub(crate) async fn verify(backend: &dyn Backend) -> Result {
    for case in [
        Case::Covering,
        Case::ReadOnlyPrivate,
        Case::ReadOnlyShared,
        Case::WrongAudience,
        Case::OtherAgent,
        Case::Expired,
        Case::Revoked,
        Case::StructuredPrivateOnly,
    ] {
        run(backend, case).await?;
    }
    Ok(())
}

pub(crate) fn volume(name: &str, agent: AgentId) -> Result<VolumeBinding> {
    Ok(VolumeBinding {
        name: name.into(),
        volume: VolumeRef::new(
            ProviderRef::new(
                "conformance".into(),
                "filesystem".into(),
                "1".into(),
                &mut None,
            )?,
            format!("root-{name}"),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(agent.to_bytes())),
            &mut None,
        )?,
    })
}

async fn run(backend: &dyn Backend, case: Case) -> Result {
    let storage = backend.fresh().await?;
    let stream = Arc::new(Observed::new(storage.open().await?));
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "workload".into(),
    };
    let issued = match case {
        Case::WrongAudience => Authority {
            kind: AggregateKind::Conversation,
            id: "other-workload".into(),
        },
        _ => audience.clone(),
    };
    let issuer = AuthorityRoot::new("conformance", [7; 32], issued.clone());
    let agent = crate::conformance::fixtures::adopted().agent;
    let private = volume(PRIVATE_VOLUME, agent.clone())?;
    let shared = volume("shared", agent.clone())?;
    let mut grants = vec![
        INTERACTION_ROUTE.to_owned(),
        private
            .volume
            .capability(VolumeOperation::Read, &mut None)?,
        shared.volume.capability(VolumeOperation::Read, &mut None)?,
    ];
    if case != Case::ReadOnlyPrivate {
        grants.push(
            private
                .volume
                .capability(VolumeOperation::Write, &mut None)?,
        );
    }
    if case != Case::ReadOnlyShared {
        grants.push(
            shared
                .volume
                .capability(VolumeOperation::Write, &mut None)?,
        );
    }
    if case == Case::StructuredPrivateOnly {
        grants = vec![
            INTERACTION_ROUTE.to_owned(),
            acyclic_fs::access::Capability::whole_volume(
                Some(private.volume.id().to_owned()),
                &acyclic_fs::access::Action::ALL,
            )
            .to_scope_label()?,
        ];
    }
    let bound_agent =
        (case == Case::OtherAgent).then(|| acyclic_harness_core::AgentId::from_bytes([9; 16]));
    let mut scope =
        issuer.bootstrap_scope("root", &Capabilities::new(grants), bound_agent, &mut None)?;
    match case {
        Case::Revoked => issuer.revoke_workload(&scope)?,
        Case::Expired => {
            let now = acyclic_stream::SystemUnixMillisClock.now_unix_millis() / 1000;
            let deadline = now + 1;
            scope = scope.with_workload_expiry(
                deadline,
                &issuer.public_workload_trust()?,
                &issued,
                now,
            )?;
            let delay = (deadline * 1000)
                .saturating_sub(acyclic_stream::SystemUnixMillisClock.now_unix_millis());
            PlatformExecutor.sleep(u32::try_from(delay + 1)?).await;
        }
        _ => {}
    }
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let objects = Arc::new(objects);
    let observed = (objects.clone(), bucket.clone());
    let bound = Bound::admitted(
        issuer,
        VerifiedCaller { audience, scope },
        Placement {
            filesystem: acyclic_fs::Fs::new(
                acyclic_fs::StreamAuthorityStore::new(stream.clone()),
                LogicalObjectStore::new(objects, bucket),
                EmbeddedCapabilities::MEMORY,
            ),
            provider: private.volume.provider().clone(),
            within: ActorReference::new(b"conformance", b"agents", b"host")?,
        },
        Turns {
            executors: Arc::new(Never),
            limits: TurnLimits {
                maximum_bytes: 64 * 1024,
                max_steps: 4,
                history_turns: 8,
            },
            actors: vectors::limits()?,
            clock: Arc::new(SystemClock),
            approval_policy: Default::default(),
        },
    )?;
    let request = || RootRequest {
        agent: agent.clone(),
        fresh: vec![private.clone(), shared.clone()],
        retained: Vec::new(),
        brief: None,
        policy: None,
    };
    let intent = vectors::intent("root")?;
    let prepared = bound.prepare_root(&intent, request()).await;
    let io = stream.reads.load(Ordering::SeqCst) + stream.writes.load(Ordering::SeqCst);
    if io != 0 {
        return Err(format!("{case:?}: root preparation performed provider IO").into());
    }
    match (case, prepared) {
        (Case::Covering, Ok(first)) => {
            let again = bound.prepare_root(&intent, request()).await?;
            if again.request() != first.request() {
                return Err("root preparation is not exact".into());
            }
            let child = first.child().clone();
            let RootSubmission::Created(created) = bound.submit_root(first).await? else {
                return Err("the root's submission was uncertain".into());
            };
            if created != child || bound.agents().state(&created).await?.volumes != request().fresh
            {
                return Err("created root differs from its preparation".into());
            }
            // The Unknown path: the identical prepared commit resubmits as the
            // same creation, and creating again under the intent replays it.
            if !matches!(
                bound.submit_root(again).await?,
                RootSubmission::Created(replayed) if replayed == child
            ) || !matches!(
                bound.create_root(&intent, request()).await?,
                RootSubmission::Created(replayed) if replayed == child
            ) {
                return Err("an exact root resubmission did not replay".into());
            }
            Ok(())
        }
        (Case::Covering, Err(error)) => {
            Err(format!("covering admission was refused: {error}").into())
        }
        (_, Ok(_)) => Err(format!("{case:?}: admission prepared a root it does not cover").into()),
        (_, Err(_)) => {
            // A refusal writes no volume object either.
            use acyclic_objects::v1::ObjectsProvider as _;
            let (objects, bucket) = observed;
            let written = objects
                .list(acyclic_objects::v1::wire::ListObjectsRequest {
                    bucket: Some(bucket),
                    ..Default::default()
                })
                .await?
                .entries
                .len();
            if written != 0 {
                return Err(
                    format!("{case:?}: a refused admission wrote {written} objects").into(),
                );
            }
            Ok(())
        }
    }
}
