use super::*;
use crate::kernel::KernelAccounting;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Promotes one resolved object under the supplied publication authority.
    pub async fn promote(
        &self,
        path: &str,
        maximum_bytes: u64,
        operation: IdempotencyKey,
        permit: crate::PublicationPermit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        let requested = self.canonical_path(path, accounting)?;
        let lookup = self.lookup(&requested.value, accounting).await?;
        let result = self
            .promote_resolved(
                &requested.value,
                &lookup.0,
                maximum_bytes,
                operation,
                permit,
                accounting,
            )
            .await;
        self.retire_lookup(lookup, accounting)?;
        requested.retire(accounting)?;
        result
    }

    async fn promote_resolved(
        &self,
        requested: &str,
        lookup: &LazyLookup,
        maximum_bytes: u64,
        operation: IdempotencyKey,
        permit: crate::PublicationPermit,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        if matches!(lookup, LazyLookup::Authored { path, .. } if path == requested) {
            return Ok(TransactionCommit::AlreadyCommitted(
                self.head(accounting).await?,
            ));
        }
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
        if let Some(parent) = parent_path(requested) {
            transaction
                .create_dirs(parent, accounting, cancellation(accounting))
                .await?;
        }
        self.apply_resolved_promotion(
            &mut transaction,
            requested,
            lookup,
            maximum_bytes,
            accounting,
        )
        .await?;
        self.commit(&mut transaction, permit, accounting).await
    }

    /// Stages an inspected identity in an unpublished transaction.
    pub async fn apply_resolved_promotion(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        requested: &str,
        lookup: &LazyLookup,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let cancellation = cancellation(accounting);
        match lookup {
            LazyLookup::Source { source, node } => {
                self.populate_source_node(
                    transaction,
                    requested,
                    *source,
                    *node,
                    maximum_bytes,
                    accounting,
                )
                .await?;
            }
            LazyLookup::Authored { path, .. } if path == requested => return Ok(false),
            LazyLookup::Authored { path, .. } => 'accounted: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    transaction
                        .hard_link(path, requested, original, cancellation)
                        .await?;
                    break 'accounted;
                }
                accept_receipt(
                    transaction
                        .hard_link(path, requested, accounting.remaining()?, cancellation)
                        .await,
                    accounting,
                )?;
            }
            LazyLookup::Shadow { record, .. } => {
                'accounted: {
                    let cancellation = cancellation;
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(original) = accounting.native_work() {
                        break 'accounted transaction
                            .restore_record(requested, *record, original, cancellation)
                            .await
                            .map_err(LazyWorkspaceError::from);
                    }
                    accept_receipt(
                        transaction
                            .restore_record(
                                requested,
                                *record,
                                accounting.remaining()?,
                                cancellation,
                            )
                            .await,
                        accounting,
                    )
                }?;
            }
        }
        Ok(true)
    }

    /// Promotes only the exact identity the caller inspected.
    pub async fn promote_exact(
        &self,
        path: &str,
        expected: FileId,
        maximum_bytes: u64,
        operation: IdempotencyKey,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<crate::Generation<A, O>, LazyWorkspaceError> {
        let lookup = self.lookup(path, accounting).await?;
        let result = async {
            if self
                .stable_file_id_for_lookup(path, &lookup.0, accounting)
                .await?
                != expected
            {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            match self
                .promote_resolved(
                    path,
                    &lookup.0,
                    maximum_bytes,
                    operation,
                    crate::PublicationPermit::Unrestricted,
                    accounting,
                )
                .await?
            {
                TransactionCommit::Committed(generation)
                | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
                TransactionCommit::IdempotencyConflict => Err(LazyWorkspaceError::Concurrent),
                TransactionCommit::Conflict { .. } | TransactionCommit::Fenced => {
                    Err(LazyWorkspaceError::StaleIdentity)
                }
            }
        }
        .await;
        self.retire_lookup(lookup, accounting)?;
        result
    }

    /// Stages a live or frozen identity in an unpublished checkout.
    #[cfg(feature = "native-mount")]
    pub(crate) async fn stage_into_checkout(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        snapshot: Option<LazySnapshotRef>,
        path: &str,
        expected: FileId,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        let lookup = if let Some(snapshot) = snapshot {
            let (lookup, _, allocation) =
                self.lookup_at_snapshot(snapshot, path, accounting).await?;
            (lookup.ok_or(LazyWorkspaceError::NotFound)?, allocation)
        } else {
            self.lookup(path, accounting).await?
        };
        let result = async {
            if self
                .stable_file_id_for_lookup(path, &lookup.0, accounting)
                .await?
                != expected
            {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            let namespace = self.namespace_path(path, accounting)?;
            let record = checkout
                .lookup_no_follow_with_metadata_with_accounting(
                    &namespace.value,
                    accounting,
                    cancellation(accounting),
                )
                .await;
            namespace.retire(accounting)?;
            if let Some(record) = record? {
                return if record.record.file_id == expected {
                    Ok(false)
                } else {
                    Err(LazyWorkspaceError::StaleIdentity)
                };
            }
            if matches!(&lookup.0, LazyLookup::Authored { path: authored, .. } if authored == path)
            {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            let candidate =
                checkout.private_candidate_with_accounting(accounting, cancellation(accounting))?;
            let mut transaction = 'accounted: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    break 'accounted {
                        crate::Transaction::for_checkout_candidate(
                            &self.workspace,
                            candidate,
                            original,
                            cancellation(accounting),
                        )?
                    };
                }
                let result = crate::Transaction::for_checkout_candidate(
                    &self.workspace,
                    candidate,
                    accounting.remaining()?,
                    cancellation(accounting),
                );
                accept_receipt(result, accounting)?
            };
            if let Some(parent) = parent_path(path) {
                transaction
                    .create_dirs(parent, accounting, cancellation(accounting))
                    .await?;
            }
            let changed = self
                .apply_resolved_promotion(
                    &mut transaction,
                    path,
                    &lookup.0,
                    maximum_bytes,
                    accounting,
                )
                .await?;
            if changed {
                *checkout = transaction.checkout;
            }
            Ok(changed)
        }
        .await;
        self.retire_lookup(lookup, accounting)?;
        result
    }
}
