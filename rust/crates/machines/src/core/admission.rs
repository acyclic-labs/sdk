//! Capability admission belongs above native image and creation primitives.
use crate::*;

/// Outbound policy enrolled by the host for a contract's network commitment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkPolicy {
    BlockAll,
    AllowDomains(Vec<String>),
}

/// Resolve and validate the committed policy before native creation. Adapters
/// only encode this admitted value in their provider's wire representation.
pub fn admit_network<'a>(
    policies: &'a BTreeMap<[u8; 32], NetworkPolicy>,
    digest: &[u8; 32],
) -> Result<&'a NetworkPolicy, ProviderError> {
    let policy = policies.get(digest).ok_or_else(|| ProviderError::Unsupported(
        "network commitment is unregistered; recovery: enroll its exact policy in the machine backend configuration before creation".into()))?;
    if let NetworkPolicy::AllowDomains(domains) = policy {
        if domains.is_empty() {
            return Err(ProviderError::Invalid(
                "network allowlist is empty; recovery: enroll BlockAll for this commitment".into(),
            ));
        }
        if domains.iter().any(|domain| {
            domain.is_empty()
                || !domain
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '*'))
        }) {
            return Err(ProviderError::Invalid(
                "network allowlist contains an unexpressible domain; recovery: enroll individual domain names without separators".into(),
            ));
        }
    }
    Ok(policy)
}

/// Construct the exact contract from the requested image and its qualification.
/// An adapter supplies native image facts, never a second compatibility policy.
pub fn admit_contract(
    request: &CreateMachine,
    qualification: ImageQualification,
) -> Result<MachineContract, ProviderError> {
    if qualification.image != request.image {
        return Err(ProviderError::Rejected(
            "image qualification identity mismatch; recovery: repair the raw image mapping before creating a machine".into(),
        ));
    }
    if let CompatibilityPolicy::Require(required) = &request.compatibility
        && (required.is_empty() || !required.is_subset(&qualification.capabilities))
    {
        return Err(ProviderError::Unsupported(format!(
            "required image capabilities are unavailable; recovery: select an image qualified for {required:?}"
        )));
    }
    let suspension = if qualification
        .capabilities
        .contains(&Capability::SuspendResume)
    {
        request.suspension
    } else {
        SuspensionPolicy::Manual
    };
    Ok(MachineContract {
        image: qualification.image,
        capabilities: qualification.capabilities,
        compatibility: request.compatibility.clone(),
        compatibility_revision: qualification.compatibility_revision,
        suspension,
        expiration: request.expiration,
        network_policy_digest: request.network_policy_digest,
        budgets: request.budgets,
    })
}
