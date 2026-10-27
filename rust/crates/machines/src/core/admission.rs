//! Capability admission belongs above native image and creation primitives.
use crate::*;

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
