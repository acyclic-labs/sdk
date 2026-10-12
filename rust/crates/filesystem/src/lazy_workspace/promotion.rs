use super::*;
impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Promotes one resolved object under the supplied publication authority.
    pub async fn promote(
        &self,
        path: &str,
        maximum: u64,
        key: IdempotencyKey,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let path = self.canonical_path(path, ctx)?;
        let result = async {
            let lookup = self.lookup(&path.value, ctx).await?;
            let result = self
                .promote_resolved(&path.value, &lookup.0, maximum, key, permit, ctx)
                .await;
            self.retire_lookup(lookup, ctx)?;
            result
        }
        .await;
        path.retire(ctx)?;
        result
    }
    async fn promote_resolved(
        &self,
        path: &str,
        lookup: &LazyLookup,
        maximum: u64,
        key: IdempotencyKey,
        permit: crate::PublicationPermit,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        if matches!(lookup,LazyLookup::Authored { path: authored,.. } if authored == path) {
            return Ok(TransactionCommit::AlreadyCommitted(self.head(ctx).await?));
        }
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
        if let Some(parent) = parent_path(path) {
            accounted!(ctx, |work, cancel| transaction
                .create_dir_all(parent, work, cancel))?;
        }
        self.apply_resolved_promotion(&mut transaction, path, lookup, maximum, ctx)
            .await?;
        self.commit(&mut transaction, permit, ctx).await
    }

    /// Stages an inspected identity in an unpublished transaction.
    pub async fn apply_resolved_promotion(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        path: &str,
        lookup: &LazyLookup,
        maximum: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        match lookup {
            LazyLookup::Authored { path: authored, .. } if authored == path => return Ok(false),
            LazyLookup::Authored { path: authored, .. } => {
                accounted!(ctx, |work, cancel| transaction
                    .hard_link(authored, path, work, cancel))?;
            }
            LazyLookup::Shadow { record, .. } => {
                let path = self.namespace_path(path, ctx)?;
                let mut operations = Owned::allocate(1, ctx)?;
                crate::Transaction::<A, O>::push_mutation(
                    &mut operations,
                    Owned::new(
                        crate::kernel::Mutation::Restore {
                            path: path.value,
                            record: *record,
                        },
                        path.allocation,
                        path.bytes,
                    ),
                    ctx,
                )?;
                transaction
                    .checkout
                    .mutate_owned_with_accounting(operations, ctx, cancellation(ctx))
                    .await?;
            }
            LazyLookup::Source { source, node } => {
                self.populate_source_node(transaction, path, *source, *node, maximum, ctx)
                    .await?
            }
        }
        Ok(true)
    }
    /// Promotes only the identity the caller resolved.
    pub async fn promote_exact(
        &self,
        path: &str,
        expected: FileId,
        maximum: u64,
        key: IdempotencyKey,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let lookup = self.lookup(path, ctx).await?;
        let result = async {
            if self.stable_file_id_for_lookup(path, &lookup.0, ctx).await? != expected {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            match self
                .promote_resolved(
                    path,
                    &lookup.0,
                    maximum,
                    key,
                    crate::PublicationPermit::Unrestricted,
                    ctx,
                )
                .await?
            {
                TransactionCommit::Committed(generation)
                | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
                TransactionCommit::IdempotencyConflict => Err(LazyWorkspaceError::Concurrent),
                _ => Err(LazyWorkspaceError::StaleIdentity),
            }
        }
        .await;
        self.retire_lookup(lookup, ctx)?;
        result
    }
    /// Stages a live or frozen identity in an unpublished mounted checkout.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn stage_into_checkout(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        cut: Option<LazySnapshotRef>,
        path: &str,
        expected: FileId,
        maximum: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let lookup = match cut {
            Some(cut) => {
                let (lookup, _, guard) = self.lookup_at_snapshot(cut, path, ctx).await?;
                (lookup.ok_or(LazyWorkspaceError::NotFound)?, guard)
            }
            None => self.lookup(path, ctx).await?,
        };
        let result = async {
            if self.stable_file_id_for_lookup(path, &lookup.0, ctx).await? != expected {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            let namespace = self.namespace_path(path, ctx)?;
            let present = checkout
                .lookup_no_follow_with_metadata_with_accounting(
                    &namespace.value,
                    ctx,
                    cancellation(ctx),
                )
                .await;
            namespace.retire(ctx)?;
            if let Some(present) = present? {
                return if present.record.file_id == expected {
                    Ok(false)
                } else {
                    Err(LazyWorkspaceError::StaleIdentity)
                };
            }
            if matches!(&lookup.0,LazyLookup::Authored { path: authored,.. } if authored == path) {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            let candidate = checkout.private_candidate_with_accounting(ctx, cancellation(ctx))?;
            let mut transaction = accounted!(ctx, |work, cancel| async {
                crate::Transaction::for_checkout_candidate(&self.workspace, candidate, work, cancel)
            })?;
            if let Some(parent) = parent_path(path) {
                accounted!(ctx, |work, cancel| transaction
                    .create_dir_all(parent, work, cancel))?;
            }
            let changed = self
                .apply_resolved_promotion(&mut transaction, path, &lookup.0, maximum, ctx)
                .await?;
            if changed {
                *checkout = transaction.checkout;
            }
            Ok(changed)
        }
        .await;
        self.retire_lookup(lookup, ctx)?;
        result
    }
}
