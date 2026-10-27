//! Count turn IO independently of scheduler IO. A valid scheduler must read
//! actor state before it knows which workload admission to verify.
use crate::conformance::{Backend, Result, observe::Observed, vectors};
use acyclic_harness_core::{
    Capabilities, OperationId,
    agent::*,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot, Scope},
    executor::Executor,
    filesystem::{
        FilesystemHost, FilesystemTurnHost, PRIVATE_VOLUME, TurnAdmission, TurnAuthority,
        TurnExecutors, TurnLimits, agent_authority,
    },
    model::ModelContent,
    resources::ProviderRef,
};
use acyclic_stream::{
    BoxProviderFuture, UnixMillisClock,
    runtime::{Executor as _, PlatformExecutor},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub(crate) mod fixture;

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
                "conformance executor sentinel".into(),
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
    Ok(())
}

async fn admission(
    root: &AuthorityRoot,
    audience: Authority,
    binding: &VolumeBinding,
    case: Case,
) -> Result<TurnAdmission> {
    if matches!(case, Case::LocalRoot) {
        return Ok(TurnAdmission::LocalRoot);
    }
    let mut grants = vec![
        binding
            .volume
            .capability(VolumeOperation::Read, &mut None)?,
    ];
    if !matches!(case, Case::ReadOnly | Case::Widened) {
        grants.push(
            binding
                .volume
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
            let now = acyclic_stream::SystemUnixMillisClock.now_unix_millis() / 1000;
            let deadline = now + 1;
            scope = scope.with_workload_expiry(
                deadline,
                &root.public_workload_trust()?,
                &audience,
                now,
            )?;
            let delay = (deadline * 1000)
                .saturating_sub(acyclic_stream::SystemUnixMillisClock.now_unix_millis());
            PlatformExecutor.sleep(u32::try_from(delay + 1)?).await;
        }
        Case::Widened => {
            let wider = Capabilities::new([
                binding
                    .volume
                    .capability(VolumeOperation::Read, &mut None)?,
                binding
                    .volume
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
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "workload".into(),
    };
    let root = AuthorityRoot::new("conformance", [7; 32], audience.clone());
    let admission = admission(&root, audience, &fixture.binding, case).await?;
    check_scope(&root, &admission, &fixture.path, &fixture.binding, case)?;
    fixture
        .check(
            backend,
            root,
            admission,
            matches!(case, Case::LocalRoot | Case::Admitted),
            &format!("{case:?}"),
        )
        .await
}
fn check_scope(
    root: &AuthorityRoot,
    admission: &TurnAdmission,
    path: &AgentPath,
    binding: &VolumeBinding,
    case: Case,
) -> Result {
    let authority = agent_authority(root, admission, path, std::slice::from_ref(binding), &[]);
    match (case, authority) {
        (Case::LocalRoot, Ok(authority))
            if authority
                .grants
                .contains(acyclic_harness_core::contract::capability::INTERACTION_ROUTE) =>
        {
            Ok(())
        }
        (Case::Admitted, Ok(authority)) => {
            let TurnAdmission::Admitted { scope, .. } = admission else {
                return Err("admitted control lost its scope".into());
            };
            if authority.scope.capabilities() != scope.capabilities()
                || authority.scope.agent()
                    != Some(acyclic_harness_core::AgentId::from_bytes(
                        path.id().to_bytes(),
                    ))
            {
                return Err("delegated scope widened or lost its agent binding".into());
            }
            Ok(())
        }
        (Case::LocalRoot | Case::Admitted, _) => {
            Err("valid admission failed authority derivation".into())
        }
        (_, Err(_)) => Ok(()),
        _ => Err("invalid admission acquired authority".into()),
    }
}
