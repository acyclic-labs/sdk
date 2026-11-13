//! Admitted root preparation: one placement builds the agents and their
//! volumes, and a root's fresh volumes and adoption are one commit prepared
//! without stream IO. Workload volumes retain the admission's ceiling. Native
//! custody owns the hidden journal; an admitted port must grant it explicitly.
use crate::conformance::{Backend, Result, observe::Observed, vectors};
use acyclic_actors::{addressing::ActorReference, local::SystemClock};
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
use acyclic_stream::{BoxProviderFuture, UnixMillisClock, testing::Counted};
use std::sync::Arc;

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
    DelegatedCovering,
    ReadOnlyPrivate,
    ReadOnlyShared,
    ReadOnlyJournal,
    NativeJournalCustody,
    WrongAudience,
    OtherAgent,
    Expired,
    Revoked,
    /// One structured grant over the private volume alone, which admits the
    /// agent but not the second fresh volume.
    StructuredPrivateOnly,
}

/// Verify root admission and exact replay against the backend's real storage.
pub async fn verify(backend: &dyn Backend) -> Result {
    for case in [
        Case::Covering,
        Case::DelegatedCovering,
        Case::ReadOnlyPrivate,
        Case::ReadOnlyShared,
        Case::ReadOnlyJournal,
        Case::NativeJournalCustody,
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

pub(crate) fn volume(role: WorkspaceRole, name: &str, agent: AgentId) -> Result<WorkspaceEntry> {
    Ok(WorkspaceEntry::initial_volume(
        role,
        name.into(),
        VolumeRef::new(
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
    ))
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
    let clock = Arc::new(super::fixtures::AuthorityClock::new());
    let issuer = AuthorityRoot::with_clock("conformance", [7; 32], issued.clone(), clock.clone());
    let agent = crate::conformance::fixtures::adopted().agent;
    let private = volume(WorkspaceRole::Private, PRIVATE_VOLUME, agent.clone())?;
    let shared = volume(WorkspaceRole::Shared, "shared", agent.clone())?;
    let journal = acyclic_harness_core::conversation::journal_volume(
        private.volume()?.provider().clone(),
        &agent,
    )?;
    let mut grants = vec![
        acyclic_harness_core::contract::capability::HARNESS_HOST.to_owned(),
        INTERACTION_ROUTE.to_owned(),
        private
            .volume()?
            .capability(VolumeOperation::Read, &mut None)?,
        shared
            .volume()?
            .capability(VolumeOperation::Read, &mut None)?,
        journal.capability(VolumeOperation::Read, &mut None)?,
    ];
    if case != Case::ReadOnlyPrivate {
        grants.push(
            private
                .volume()?
                .capability(VolumeOperation::Write, &mut None)?,
        );
    }
    if case != Case::ReadOnlyShared {
        grants.push(
            shared
                .volume()?
                .capability(VolumeOperation::Write, &mut None)?,
        );
    }
    if !matches!(case, Case::ReadOnlyJournal | Case::NativeJournalCustody) {
        grants.push(journal.capability(VolumeOperation::Write, &mut None)?);
    }
    if case == Case::StructuredPrivateOnly {
        grants = vec![
            acyclic_harness_core::contract::capability::HARNESS_HOST.to_owned(),
            INTERACTION_ROUTE.to_owned(),
            acyclic_fs::access::Capability::whole_volume(
                Some(private.volume()?.id().to_owned()),
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
            let now = clock.now_unix_millis() / 1000;
            let deadline = now + 1;
            scope = scope.with_workload_expiry(
                deadline,
                &issuer.public_workload_trust()?,
                &issued,
                now,
            )?;
        }
        _ => {}
    }
    let objects = storage.objects().await?;
    let capabilities = objects.capabilities;
    let objects = Counted::new(objects.into_store(storage.as_ref())?);
    let object_requests = objects.ledger();
    let placement = Placement {
        filesystem: acyclic_fs::Fs::new(
            acyclic_fs::StreamAuthorityStore::new(stream.clone()),
            objects,
            capabilities,
        ),
        provider: private.volume()?.provider().clone(),
        within: ActorReference::new(b"conformance", b"agents", b"host")?,
    };
    let turns = Turns {
        routing: None,
        executors: Arc::new(Never),
        limits: TurnLimits {
            maximum_bytes: Some(64 * 1024),
            history_turns: 8,
        },
        actors: vectors::limits()?,
        clock: Arc::new(SystemClock),
        approval_policy: Default::default(),
    };
    let (_running, stopped) = futures::channel::oneshot::channel::<()>();
    let bound = if matches!(case, Case::DelegatedCovering | Case::ReadOnlyJournal) {
        // The same native issuer stays behind its admitted operation port.
        // This is not native enrollment: only the original caller's grant
        // can authorize journal creation, even though it has the host role.
        let context = acyclic_auth::operation::Context::from_stopped(
            issuer
                .verifier(&mut None)?
                .expiry(&scope)?
                .saturating_mul(1000),
            async move {
                let _ = stopped.await;
                acyclic_auth::operation::Stopped::Cancelled
            },
        );
        Bound::platform_admitted(
            acyclic_harness_core::core::RuntimeAuthority::admitted(
                issuer.admit(scope, context)?,
                OperationId::from_bytes([9; 16]),
            ),
            placement,
            turns,
        )?
    } else {
        Bound::admitted(issuer, VerifiedCaller { audience, scope }, placement, turns)?
    };
    let request = || RootRequest {
        peer: true,
        agent: agent.clone(),
        fresh: vec![private.clone(), shared.clone()],
        retained: Vec::new(),
        brief: None,
        policy: None,
    };
    let intent = vectors::intent("root")?;
    if case == Case::Expired {
        // Exercise the original preparation before advancing its issuing clock.
        // A failed or manufactured admission cannot pass this positive control.
        let _before_expiry = bound.prepare_root(&intent, request()).await?;
        if object_requests.requests().data() == 0 {
            return Err("successful root preparation escaped object dispatch observation".into());
        }
        clock.advance_to((clock.now_unix_millis() / 1000 + 1) * 1000);
    }
    let before = object_requests.requests();
    let prepared = bound.prepare_root(&intent, request()).await;
    let requests = stream.requests.requests();
    let io = requests.data() + requests.control();
    if io != 0 {
        return Err(format!("{case:?}: root preparation performed provider IO").into());
    }
    match (case, prepared) {
        (Case::Covering | Case::DelegatedCovering | Case::NativeJournalCustody, Ok(first)) => {
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
        (Case::Covering | Case::DelegatedCovering | Case::NativeJournalCustody, Err(error)) => {
            Err(format!("covering admission was refused: {error}").into())
        }
        (_, Ok(_)) => Err(format!("{case:?}: admission prepared a root it does not cover").into()),
        (_, Err(_)) => {
            // The successful control may already have prepared immutable objects.
            // Refusal must dispatch no object work, even a deduplicated put.
            let refused = object_requests.requests().since(before);
            if refused.data() + refused.control() != 0 {
                return Err(format!("{case:?}: a refused admission dispatched object work").into());
            }
            Ok(())
        }
    }
}
