use crate::{Error, Result};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Workspace};

pub(crate) async fn replace_policy<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    workspace: &Workspace<A, O>,
    policy: &acyclic_fs::access::Policy,
) -> Result<()> {
    let cancel = acyclic_fs::CancellationToken::new();
    let mut checkout = workspace
        .checkout(
            acyclic_fs::model::GenerationSelector::Head,
            acyclic_fs::model::CheckoutMode::tracking_transaction(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| Error::from(failure.error))?
        .value;
    checkout
        .write_access_policy(
            &serde_json::to_vec(policy).map_err(|error| Error::Invalid(error.to_string()))?,
            acyclic_fs::NamedAttributeWriteMode::Replace,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| Error::from(failure.error))?;
    let result = checkout
        .commit::<acyclic_fs::FsError, _, _>(
            acyclic_fs::OperationId::new(),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancel,
            |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .map_err(|failure| Error::from(failure.error))?
        .value;
    assert!(matches!(
        result,
        acyclic_fs::LivePublicationObservation::Committed { .. }
    ));
    Ok(())
}
