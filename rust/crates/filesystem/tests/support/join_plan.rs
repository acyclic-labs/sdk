use acyclic_fs::{
    ApplyOptions, AsyncAuthorityStore, AsyncObjectStore, IdempotencyKey, JoinOutcome, JoinPlan,
    PublicationPermit, WorkspaceError,
};

pub(crate) async fn apply_planned<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    plan: &JoinPlan<A, O>,
    idempotency_key: IdempotencyKey,
) -> Result<JoinOutcome<A, O>, WorkspaceError> {
    plan.apply(
        ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key,
        },
        std::collections::BTreeMap::new(),
        PublicationPermit::Unrestricted,
    )
    .await
}
