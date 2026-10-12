use super::*;
use crate::kernel::KernelAccounting;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Replaces one complete file while retaining its source identity.
    pub async fn write(
        &self,
        path: &str,
        bytes: acyclic_stream::OwnedBytes,
        operation: IdempotencyKey,
        permit: crate::PublicationPermit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let lookup = match self.lookup(path, accounting).await {
            Ok(lookup) => Some(lookup),
            Err(LazyWorkspaceError::NotFound) => None,
            Err(error) => return Err(error),
        };
        let result = async {
            let checkout = self
                .workspace
                .checkout_accounted(
                    crate::model::GenerationSelector::Head,
                    crate::model::CheckoutMode::tracking_transaction(),
                    accounting,
                    cancellation(accounting),
                )
                .await?;
            let mut transaction =
                crate::Transaction::from_checkout(&self.workspace, checkout, operation, false);
            if let Some(parent) = parent_path(path) {
                transaction
                    .create_dirs(parent, accounting, cancellation(accounting))
                    .await?;
            }
            if let Some((lookup, _)) = &lookup
                && matches!(
                    lookup,
                    LazyLookup::Shadow { .. } | LazyLookup::Authored { .. }
                )
            {
                self.apply_resolved_promotion(&mut transaction, path, lookup, u64::MAX, accounting)
                    .await?;
            }
            'accounted: {
                let cancellation = cancellation(accounting);
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    break 'accounted transaction
                        .write(path, bytes, original, cancellation)
                        .await
                        .map_err(LazyWorkspaceError::from);
                }
                accept_receipt(
                    transaction
                        .write(path, bytes, accounting.remaining()?, cancellation)
                        .await,
                    accounting,
                )
            }?;
            if let Some((LazyLookup::Source { node, .. }, _)) = &lookup {
                let file_id = self.source_file_id(&node.file_identity, accounting)?;
                'accounted: {
                    let cancellation = cancellation(accounting);
                    let build = |path| AuthoredMutation::Reidentify { path, file_id };
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(original) = accounting.native_work() {
                        break 'accounted transaction
                            .mutate(path, |path| Ok(build(path)), original, cancellation)
                            .await
                            .map_err(LazyWorkspaceError::from);
                    }
                    accept_receipt(
                        transaction
                            .mutate(
                                path,
                                |path| Ok(build(path)),
                                accounting.remaining()?,
                                cancellation,
                            )
                            .await,
                        accounting,
                    )
                }?;
            }
            self.commit(&mut transaction, permit, accounting).await
        }
        .await;
        if let Some(lookup) = lookup {
            self.retire_lookup(lookup, accounting)?;
        }
        result
    }
}
