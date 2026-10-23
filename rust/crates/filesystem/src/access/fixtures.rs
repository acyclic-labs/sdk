use crate::*;

pub(crate) async fn publish_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    policy: &super::Policy,
) -> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let mut checkout = workspace
        .checkout(
            model::GenerationSelector::Head,
            model::CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    checkout
        .write_access_policy(
            &serde_json::to_vec(policy)?,
            NamedAttributeWriteMode::Upsert,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?;
    let result = checkout
        .commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancel,
            |_| async {
                Ok::<_, OperationFailure<FsError>>(OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await?
        .value;
    assert!(matches!(
        result,
        LivePublicationObservation::Committed { .. }
    ));
    Ok(())
}

pub(crate) fn denied(action: super::Action, path: &str) -> super::Policy {
    let mut policy: super::Policy = serde_json::from_slice(super::INITIAL_POLICY).unwrap();
    policy.version += 1;
    let mut rule = policy.layers[0][0].clone();
    rule.id = "restricted".into();
    rule.effect = super::Effect::Deny;
    rule.actions = [action].into_iter().collect();
    rule.resource.path = path.into();
    policy.layers[0].push(rule);
    policy
}
