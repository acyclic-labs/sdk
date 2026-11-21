use acyclic_fs::access::Policy;
use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use {
    acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Workspace},
    acyclic_fs_work::performance::WorkBudget,
};

pub async fn publish<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    policy: &Policy,
) -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    checkout
        .write_access_policy(
            &serde_json::to_vec(&policy)?,
            acyclic_fs::NamedAttributeWriteMode::Replace,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    checkout
        .commit(
            acyclic_fs_format::identity::OperationId::new(),
            acyclic_fs::PublicationIntent::guarded(
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
            ),
            WorkBudget::UNBOUNDED,
            &cancellation,
            |_: WorkBudget| {
                std::future::ready(Ok::<
                    _,
                    acyclic_fs_work::performance::OperationFailure<acyclic_fs::FsError>,
                >(
                    acyclic_fs_work::performance::OperationReceipt {
                        value: (),
                        work: Default::default(),
                    },
                ))
            },
        )
        .await?;
    Ok(())
}
