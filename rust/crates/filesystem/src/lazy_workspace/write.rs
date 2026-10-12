use super::*;
impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Replaces a complete file while preserving its source identity.
    pub async fn write(
        &self,
        path: &str,
        bytes: acyclic_stream::OwnedBytes,
        key: IdempotencyKey,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let lookup = match self.lookup(path, ctx).await {
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
                    ctx,
                    cancellation(ctx),
                )
                .await?;
            let mut transaction =
                crate::Transaction::from_checkout(&self.workspace, checkout, key, false);
            self.stage_write(
                &mut transaction,
                path,
                bytes,
                lookup.as_ref().map(|(lookup, _)| lookup),
                ctx,
            )
            .await?;
            self.commit(&mut transaction, permit, ctx).await
        }
        .await;
        if let Some(lookup) = lookup {
            self.retire_lookup(lookup, ctx)?;
        }
        result
    }
    pub(super) async fn stage_write(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        path: &str,
        bytes: acyclic_stream::OwnedBytes,
        lookup: Option<&LazyLookup>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        if let Some(parent) = parent_path(path) {
            transaction
                .create_dirs(parent, ctx, cancellation(ctx))
                .await?;
        }
        if let Some(lookup) = lookup
            && matches!(
                lookup,
                LazyLookup::Authored { .. } | LazyLookup::Shadow { .. }
            )
        {
            self.apply_resolved_promotion(transaction, path, lookup, u64::MAX, ctx)
                .await?;
        }
        accounted!(ctx, |work, cancel| transaction
            .write(path, bytes, work, cancel))?;
        if let Some(LazyLookup::Source { node, .. }) = lookup {
            let file_id = self.source_file_id(&node.file_identity, ctx)?;
            accounted!(ctx, |work, cancel| transaction.mutate(
                path,
                |path| Ok::<_, WorkspaceError>(AuthoredMutation::Reidentify { path, file_id }),
                work,
                cancel
            ))?;
        }
        Ok(())
    }
}
