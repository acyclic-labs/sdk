use crate::*;

/// Publishes the policy through a filesystem transaction.
pub async fn publish_policy<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
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
            crate::PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
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
    if matches!(result, LivePublicationObservation::Committed { .. }) {
        Ok(())
    } else {
        Err("the policy publication was not committed".into())
    }
}

/// Builds a policy with a deny rule for the requested action and path.
pub fn denied(action: super::Action, path: &str) -> Result<super::Policy, serde_json::Error> {
    let mut policy: super::Policy = serde_json::from_slice(super::INITIAL_POLICY)?;
    policy.version += 1;
    let rules = policy
        .layers
        .first_mut()
        .ok_or_else(|| serde::de::Error::custom("the initial policy has no rule layer"))?;
    let mut rule = rules
        .first()
        .cloned()
        .ok_or_else(|| serde::de::Error::custom("the initial policy has no rule to restrict"))?;
    rule.id = "restricted".into();
    rule.effect = super::Effect::Deny;
    rule.actions = [action].into_iter().collect();
    rule.resource.path = path.into();
    rules.push(rule);
    Ok(policy)
}
