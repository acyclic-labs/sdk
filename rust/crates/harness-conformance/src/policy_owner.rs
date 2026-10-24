//! Portable policy-owner fixture; verdicts remain in the substrate runner.
use crate::TestResult;
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Fs, WorkBudget, Workspace,
    access::{Action, Effect, Policy},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Case {
    expect: Observation,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Raw results of authorization, publication and durable policy reopening.
pub struct Observation {
    /// Whether an initially authorized fork published successfully.
    pub forked: bool,
    /// Whether a reopened source denied the same fork after revocation.
    pub revoked: bool,
    /// Version read from the reopened workspace's policy attribute.
    pub policy_version: u64,
}

impl Case {
    pub(crate) fn parse(value: serde_json::Value) -> TestResult<Self> {
        Ok(serde_json::from_value(value)?)
    }
    pub(crate) fn check(&self, actual: &Observation) -> TestResult {
        if actual != &self.expect {
            return Err(format!(
                "policy-owner observations differ: {actual:?}; expected {:?}",
                self.expect
            )
            .into());
        }
        Ok(())
    }
}

// This portable driver also owns a composed policy/fork operation. Keep its
// concrete filesystem future behind a boundary before collect awaits it.
fn fork<'a, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore>(
    source: &'a Workspace<A, O>,
    name: &'a str,
) -> acyclic_stream::BoxProviderFuture<'a, Result<Workspace<A, O>, acyclic_fs::WorkspaceError>> {
    Box::pin(async move {
        let cancellation = CancellationToken::new();
        let result = source
            .with_stable_policy(WorkBudget::UNBOUNDED, &cancellation, || async {
                let bound = source
                    .clone()
                    .bind_coarse_access(&[Action::Read, Action::List, Action::Fork], &cancellation)
                    .await?;
                let generation = bound
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| failure.error)?
                    .value;
                bound
                    .fork::<_, _, acyclic_fs::WorkspaceError>(
                        name,
                        acyclic_fs::ForkOptions::from_generation(
                            generation,
                            acyclic_fs::IdempotencyKey::new(),
                        ),
                        acyclic_fs::PublicationPermit::Unrestricted,
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                        &mut |_| {
                            std::future::ready(Ok(acyclic_fs::OperationReceipt {
                                value: (),
                                work: acyclic_fs::WorkCounters::UNCHARGED,
                            }))
                        },
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
            })
            .await;
        result.map_err(|failure| failure.error)?.value
    })
}

fn revoked_policy() -> TestResult<Vec<u8>> {
    let mut policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
    policy.version += 1;
    let layer = policy
        .layers
        .first_mut()
        .ok_or("initial policy has no layer")?;
    let mut deny = layer
        .first()
        .ok_or("initial policy layer is empty")?
        .clone();
    deny.id = "revoke-fork".into();
    deny.effect = Effect::Deny;
    deny.actions = [Action::Fork].into_iter().collect();
    layer.push(deny);
    Ok(serde_json::to_vec(&policy)?)
}

pub(crate) async fn collect<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore>(
    mut recipe: serde_json::Value,
    filesystem: Fs<A, O>,
) -> TestResult<Observation> {
    recipe
        .as_object_mut()
        .ok_or("policy-owner recipe must be an object")?
        .remove("driver");
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {}
    let _: Input = serde_json::from_value(recipe)?;
    let cancellation = CancellationToken::new();
    let lifecycle = if filesystem.capabilities().durable {
        acyclic_fs::Lifecycle::Durable
    } else {
        acyclic_fs::Lifecycle::Ephemeral
    };
    let source = filesystem
        .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
            "policy-owner-source",
            acyclic_fs::VolumeInitialization {
                config: acyclic_fs::VolumeConfig::portable(lifecycle),
                access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
            },
            acyclic_fs::OperationId::new(),
            WorkBudget::UNBOUNDED,
            &cancellation,
            None,
        )
        .await?
        .value
        .workspace();
    let forked = fork(&source, "policy-owner-child").await.is_ok();
    let mut checkout = source
        .checkout(
            acyclic_fs::model::GenerationSelector::Head,
            acyclic_fs::model::CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    checkout
        .write_access_policy(
            &revoked_policy()?,
            acyclic_fs::NamedAttributeWriteMode::Upsert,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    checkout
        .commit(
            acyclic_fs::OperationId::new(),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            |_| async {
                Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    },
                )
            },
        )
        .await?;
    drop((checkout, source));
    let reopened = filesystem
        .open_workspace("policy-owner-source", WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let revoked = matches!(
        fork(&reopened, "policy-owner-revoked").await,
        Err(acyclic_fs::WorkspaceError::AccessDenied)
    );
    let mut checkout = reopened
        .checkout(
            acyclic_fs::model::GenerationSelector::Head,
            acyclic_fs::model::CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let stored = checkout
        .read_access_policy(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .0
        .ok_or("persisted policy missing")?;
    let stored: Policy = serde_json::from_slice(&stored.bytes)?;
    Ok(Observation {
        forked,
        revoked,
        policy_version: stored.version,
    })
}
