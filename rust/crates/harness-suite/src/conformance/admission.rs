//! Count turn IO independently of scheduler IO. A valid scheduler must read
//! actor state before it knows which workload admission to verify.
use crate::conformance::{Backend, Result, observe::Observed, vectors};
use acyclic_harness_core::{
    Capabilities, OperationId,
    agent::*,
    conversation::VolumeOperation,
    core::{AggregateKind, Authority, AuthorityRoot, Scope},
    executor::Executor,
    filesystem::{
        FilesystemHost, FilesystemTurnHost, PRIVATE_VOLUME, TurnAdmission, TurnAuthority,
        TurnExecutors, TurnLimits, agent_authority,
    },
    model::ModelContent,
};
use acyclic_stream::{BoxProviderFuture, UnixMillisClock};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub(crate) mod fixture;

pub(super) const EXECUTOR_SENTINEL: &str = "conformance executor sentinel";

struct Executors(AtomicUsize);
impl TurnExecutors for Executors {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        _: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Arc<dyn Executor>>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(acyclic_harness_core::Error::Invalid(
                EXECUTOR_SENTINEL.into(),
            ))
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum Case {
    LocalRoot,
    Admitted,
    ReadOnly,
    WrongAudience,
    OtherAgent,
    Revoked,
    Expired,
    Widened,
}

pub(crate) async fn verify(backend: &dyn Backend) -> Result {
    for case in [
        Case::LocalRoot,
        Case::Admitted,
        Case::ReadOnly,
        Case::WrongAudience,
        Case::OtherAgent,
        Case::Revoked,
        Case::Expired,
        Case::Widened,
    ] {
        run(backend, case).await?;
    }
    workload_custody().await
}

/// A workload derives only for its own journal's agent, holds neither
/// `HARNESS_HOST` nor its parent's structured label, and never reaches
/// another agent's journal or private volume, even when that label covers
/// every volume the host selects.
async fn workload_custody() -> Result {
    use acyclic_harness_core::{
        contract::capability::HARNESS_HOST,
        conversation::ContentGrant,
        core::{RuntimeAuthority, ScopeDerivation},
    };
    let owner = AgentId::from_bytes([31; 16]);
    let other = AgentId::from_bytes([32; 16]);
    let provider = acyclic_harness_core::resources::ProviderRef::new(
        "workload".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let journal = acyclic_harness_core::conversation::journal_volume(provider.clone(), &owner)?;
    let foreign = acyclic_harness_core::conversation::journal_volume(provider.clone(), &other)?;
    let private = acyclic_harness_core::filesystem::private_volume(provider.clone(), &owner)?;
    let foreign_private = acyclic_harness_core::filesystem::private_volume(provider, &other)?;
    // A fork binds its source's private volume beside its own.
    let volumes = [
        WorkspaceEntry::initial_volume(
            WorkspaceRole::Private,
            PRIVATE_VOLUME.into(),
            private.clone(),
        ),
        WorkspaceEntry::initial_volume(
            WorkspaceRole::Output,
            "source".into(),
            foreign_private.clone(),
        ),
    ];
    let root = AuthorityRoot::new(
        "workload",
        [33; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "workload".into(),
        },
    );
    let native: RuntimeAuthority = root.clone().into();
    let verifier = root.verifier(&mut None)?;
    let agent = |id: AgentId| acyclic_harness_core::AgentId::from_bytes(id.to_bytes());
    let read = journal.capability(VolumeOperation::Read, &mut None)?;
    let write = journal.capability(VolumeOperation::Write, &mut None)?;
    let tree = acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL)
        .to_scope_label()
        .map_err(|error| error.to_string())?;
    let workload = || ScopeDerivation::Workload {
        journal: &journal,
        volumes: &volumes,
        external: &[],
    };
    let refused = root.bootstrap_scope(
        "custody",
        &Capabilities::new([read.clone(), write.clone()]),
        Some(agent(other)),
        &mut None,
    )?;
    if native.delegate(&refused, "agent", workload()).await.is_ok() {
        return Err("a workload derived for another agent's journal".into());
    }
    let expected_owner = agent(owner);
    for grants in [
        vec![HARNESS_HOST.to_owned(), read.clone(), write.clone()],
        vec![HARNESS_HOST.to_owned(), tree.clone()],
    ] {
        let parent =
            root.bootstrap_scope("custody", &Capabilities::new(grants), None, &mut None)?;
        let derived = native.delegate(&parent, "agent", workload()).await?;
        if [HARNESS_HOST, tree.as_str()]
            .iter()
            .any(|label| derived.capabilities().contains(label))
            || derived.agent().as_ref() != Some(&expected_owner)
        {
            return Err("a derived workload kept host custody or lost its owner".into());
        }
        // A guest never reaches another agent's journal or private volume,
        // even when its parent's structured label covers every volume.
        for volume in [&foreign, &foreign_private] {
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                if ContentGrant::verify(&verifier, &derived, volume, operation, &mut None).is_ok() {
                    return Err("a workload reached another agent's private volume".into());
                }
            }
        }
    }
    Ok(())
}

async fn admission(
    root: &AuthorityRoot,
    audience: Authority,
    binding: &WorkspaceEntry,
    case: Case,
    clock: &super::fixtures::AuthorityClock,
) -> Result<TurnAdmission> {
    if matches!(case, Case::LocalRoot) {
        return Ok(TurnAdmission::LocalRoot);
    }
    let mut grants = vec![
        binding
            .volume()?
            .capability(VolumeOperation::Read, &mut None)?,
    ];
    let journal = journal(binding)?;
    grants.push(acyclic_harness_core::contract::capability::HARNESS_HOST.to_owned());
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        grants.push(journal.capability(operation, &mut None)?);
    }
    if !matches!(case, Case::ReadOnly | Case::Widened) {
        grants.push(
            binding
                .volume()?
                .capability(VolumeOperation::Write, &mut None)?,
        );
    }
    let agent = matches!(case, Case::OtherAgent)
        .then(|| acyclic_harness_core::AgentId::from_bytes([9; 16]));
    let mut scope =
        root.bootstrap_scope("admission", &Capabilities::new(grants), agent, &mut None)?;
    match case {
        Case::Revoked => root.revoke_workload(&scope)?,
        Case::Expired => {
            let now = clock.now_unix_millis() / 1000;
            let deadline = now + 1;
            scope = scope.with_workload_expiry(
                deadline,
                &root.public_workload_trust()?,
                &audience,
                now,
            )?;
        }
        Case::Widened => {
            let wider = Capabilities::new([
                binding
                    .volume()?
                    .capability(VolumeOperation::Read, &mut None)?,
                binding
                    .volume()?
                    .capability(VolumeOperation::Write, &mut None)?,
            ]);
            if root
                .issuer(&mut None)?
                .issue(&scope, "widened", &wider)
                .is_ok()
            {
                return Err("scope issuance allowed widening".into());
            }
            // Tamper with a real signed admission's claims. Its authentic chain
            // must not authorize a widening deserialized from the wire.
            let mut encoded = serde_json::to_value(&scope)?;
            *encoded
                .get_mut("capabilities")
                .ok_or("scope encoding omitted capabilities")? = serde_json::to_value(wider)?;
            scope = serde_json::from_value::<Scope>(encoded)?;
        }
        _ => {}
    }
    let audience = if matches!(case, Case::WrongAudience) {
        Authority {
            kind: audience.kind,
            id: "other-workload".into(),
        }
    } else {
        audience
    };
    Ok(TurnAdmission::Admitted { audience, scope })
}

async fn run(backend: &dyn Backend, case: Case) -> Result {
    let fixture = fixture::Fixture::new(backend).await?;
    // Continue under the conversation audience that authenticated the seeded
    // journal; changing it would invalidate its retained checkpoints.
    let audience = fixture.audience();
    let clock = Arc::new(super::fixtures::AuthorityClock::new());
    let root = AuthorityRoot::with_clock("conformance", [7; 32], audience.clone(), clock.clone());
    let admission = admission(&root, audience, &fixture.binding, case, &clock).await?;
    if matches!(case, Case::Expired) {
        check_scope(
            &root,
            &admission,
            &fixture.path,
            &fixture.binding,
            Case::Admitted,
        )
        .await?;
        clock.advance_to((clock.now_unix_millis() / 1000 + 1) * 1000);
    }
    let expected = check_scope(&root, &admission, &fixture.path, &fixture.binding, case).await?;
    fixture
        .check(
            backend,
            root,
            admission,
            matches!(case, Case::LocalRoot | Case::Admitted),
            &format!("{case:?}"),
            &expected,
        )
        .await
}
/// The implicit journal of the agent that owns `binding`'s volume.
fn journal(binding: &WorkspaceEntry) -> Result<acyclic_harness_core::conversation::VolumeRef> {
    let acyclic_harness_core::conversation::VolumeOwner::Agent(owner) = binding.volume()?.owner()
    else {
        return Err("private fixture has no agent owner".into());
    };
    Ok(acyclic_harness_core::conversation::journal_volume(
        binding.volume()?.provider().clone(),
        &AgentId::from_bytes(owner.into_bytes()),
    )?)
}

async fn check_scope(
    root: &AuthorityRoot,
    admission: &TurnAdmission,
    path: &AgentPath,
    binding: &WorkspaceEntry,
    case: Case,
) -> Result<String> {
    let authority = agent_authority(
        &root.clone().into(),
        admission,
        path,
        std::slice::from_ref(binding),
        &[],
    )
    .await;
    match (case, authority) {
        (Case::LocalRoot, Ok(authority))
            if authority
                .grants
                .contains(acyclic_harness_core::contract::capability::INTERACTION_ROUTE) =>
        {
            Ok(acyclic_harness_core::Error::Invalid(EXECUTOR_SENTINEL.into()).to_string())
        }
        (Case::Admitted, Ok(authority)) => {
            let TurnAdmission::Admitted { scope, .. } = admission else {
                return Err("admitted control lost its scope".into());
            };
            // The workload keeps every admitted grant except host custody: the
            // host role and the journal write. It reads its journal; only the
            // host writes it.
            let journal_write = journal(binding)?.capability(VolumeOperation::Write, &mut None)?;
            let guest = Capabilities::new(
                scope
                    .capabilities()
                    .iter()
                    .filter(|label| {
                        *label != acyclic_harness_core::contract::capability::HARNESS_HOST
                            && **label != journal_write
                    })
                    .map(str::to_owned),
            );
            let agent = Some(acyclic_harness_core::AgentId::from_bytes(
                path.id().to_bytes(),
            ));
            if authority.scope.capabilities() != &guest || authority.scope.agent() != agent {
                return Err(format!(
                    "delegated scope is not the admitted workload scope: granted={:?} \
                     expected={guest:?}, agent={:?} expected={agent:?}",
                    authority.scope.capabilities(),
                    authority.scope.agent(),
                )
                .into());
            }
            Ok(acyclic_harness_core::Error::Invalid(EXECUTOR_SENTINEL.into()).to_string())
        }
        (Case::LocalRoot | Case::Admitted, _) => {
            Err("valid admission failed authority derivation".into())
        }
        (_, Err(error)) => Ok(error.to_string()),
        _ => Err("invalid admission acquired authority".into()),
    }
}
