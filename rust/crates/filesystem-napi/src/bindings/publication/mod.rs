use super::*;

/// Successful authored mutation with exact work evidence.
#[napi(object)]
pub struct NativeMutationResult {
    /// New file identity for create operations.
    pub file_id: Option<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Conditional publication outcome with exact authority facts.
#[napi(object)]
pub struct NativeCommitResult {
    /// `committed`, `already-committed`, `conflict`, `fenced`, or `idempotency-conflict`.
    pub status: String,
    /// Candidate generation for successful publication.
    pub generation_id: Option<Buffer>,
    /// Relevant active authority epoch.
    pub epoch: Option<BigInt>,
    /// Relevant authority sequence.
    pub sequence: Option<BigInt>,
    /// Existing fingerprint for an idempotency conflict.
    pub committed_fingerprint: Option<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Observation-safe head-rebase outcome.
#[napi(object)]
pub struct NativeRebaseResult {
    /// `safe` or `conflicted`.
    pub status: String,
    /// New base generation when safe.
    pub generation_id: Option<Buffer>,
    /// Number of exact changed regions returned.
    pub conflict_count: u32,
    /// Whether additional conflicts exceeded the caller bound.
    pub truncated: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Terminal result of publishing one explicitly staged direct-live transaction.
#[napi(object)]
pub struct NativeLiveMutationResult {
    /// `committed`, `already-committed`, `conflicted`, `retry-limit`, `fenced`, or `idempotency-conflict`.
    pub status: String,
    /// Published generation when successful.
    pub generation_id: Option<Buffer>,
    /// Relevant authority epoch.
    pub epoch: Option<BigInt>,
    /// Relevant authority sequence.
    pub sequence: Option<BigInt>,
    /// Number of exact conflicts returned.
    pub conflict_count: u32,
    /// Whether additional conflicts exceeded the caller bound.
    pub truncated: bool,
    /// Existing fingerprint for operation-identity reuse with different content.
    pub committed_fingerprint: Option<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Observation-safe retained transaction rebase result.
#[napi(object)]
pub struct NativeTransactionRebaseResult {
    /// `rebased` or `conflicted`.
    pub status: String,
    /// New immutable base generation when safe.
    pub generation_id: Option<Buffer>,
    /// Exact bounded changed regions.
    pub conflicts: Vec<NativeTransactionConflict>,
    /// Whether additional conflicts exceeded the caller bound.
    pub truncated: bool,
}

#[napi]
impl NativeCheckout {
    /// Builds an immutable candidate generation without publishing authority.
    #[napi]
    pub async fn checkpoint(&self) -> Result<NativeCheckpointResult> {
        let checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout
            .checkpoint(acyclic_fs::performance::BOUNDARY_BUDGET, &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeCheckpointResult {
            generation_id: Buffer::from(receipt.value.digest().into_bytes().to_vec()),
            work_json: work_json(&receipt.work)?,
        })
    }

    /// Explicitly advances a clean manual checkout to the authority head.
    #[napi]
    pub async fn refresh_head(&self) -> Result<NativeCheckpointResult> {
        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .refresh_head(acyclic_fs::performance::BOUNDARY_BUDGET, &self.cancellation)
            .await
            .map_err(napi_error)?;
        encode_checkpoint_receipt(&receipt)
    }

    /// Explicitly performs observation-safe synchronization for a live checkout.
    #[napi]
    pub async fn refresh_live(&self) -> Result<NativeCheckpointResult> {
        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .refresh_live(acyclic_fs::performance::BOUNDARY_BUDGET, &self.cancellation)
            .await
            .map_err(napi_error)?;
        encode_checkpoint_receipt(&receipt)
    }

    /// Checkpoints and conditionally publishes this private overlay.
    #[napi]
    pub async fn commit(
        &self,
        operation_id: Buffer,
        lease: Option<NativeOperationWindowLease>,
    ) -> Result<NativeCommitResult> {
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let permit = publication_permit(lease.as_ref())?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout
            .retain_operation_id(operation_id)
            .map_err(napi_error)?;
        let receipt = checkout
            .commit(
                operation_id,
                None,
                None,
                None,
                permit,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
                |_| async {
                    acyclic_fs::FsResult::Ok(acyclic_fs::FsReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await;
        let receipt = match receipt {
            Ok(receipt) => {
                checkout.clear_retained_operation(operation_id);
                receipt
            }
            Err(error) => return Err(napi_error(error)),
        };
        commit_result(receipt.value, receipt.work)
    }

    /// Applies and publishes one direct-live transaction with bounded safe retries.
    #[napi]
    pub async fn mutate_live(
        &self,
        operations: Vec<NativeTransactionOperation>,
        operation_id: Buffer,
        maximum_attempts: NapiU32,
        maximum_conflicts: NapiU32,
    ) -> Result<NativeAuthoredLiveMutationResult> {
        let maximum_attempts = maximum_attempts.into_inner();
        let maximum_conflicts = maximum_conflicts.into_inner();

        let maximum =
            usize::try_from(self.config.limits.maximum_mutations_per_batch).unwrap_or(usize::MAX);
        if operations.len() > maximum || operations.capacity() > maximum {
            return Err(Error::new(
                Status::InvalidArg,
                "transaction exceeds the configured mutation bound",
            ));
        }
        let authored = operations
            .into_iter()
            .map(|operation| native_authored_transaction(operation, self.config))
            .collect::<Result<Vec<_>>>()?;
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        checkout
            .retain_operation_id(operation_id)
            .map_err(napi_error)?;
        let receipt = checkout
            .apply_authored_live(
                authored,
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await;
        match receipt {
            Ok(receipt) => {
                let resolved = live_outcome_resolved(&receipt.value.outcome);
                let created_file_ids = receipt
                    .value
                    .created_file_ids
                    .into_iter()
                    .map(|identity| identity.map(|value| Buffer::from(value.into_bytes().to_vec())))
                    .collect();
                let result = live_mutation_result(receipt.value.outcome, receipt.work)?;
                if resolved {
                    checkout.clear_retained_operation(operation_id);
                }
                Ok(NativeAuthoredLiveMutationResult {
                    created_file_ids,
                    status: result.status,
                    generation_id: result.generation_id,
                    epoch: result.epoch,
                    sequence: result.sequence,
                    conflict_count: result.conflict_count,
                    truncated: result.truncated,
                    committed_fingerprint: result.committed_fingerprint,
                    work_json: result.work_json,
                })
            }
            Err(error) => {
                if !checkout.has_pending_mutations() {
                    checkout.clear_retained_operation(operation_id);
                }
                Err(napi_error(error))
            }
        }
    }

    /// Resumes an unresolved direct-live transaction with bounded safe retries.
    #[napi]
    pub async fn resume_live(
        &self,
        operation_id: Buffer,
        maximum_attempts: NapiU32,
        maximum_conflicts: NapiU32,
    ) -> Result<NativeLiveMutationResult> {
        let maximum_attempts = maximum_attempts.into_inner();
        let maximum_conflicts = maximum_conflicts.into_inner();

        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout
            .retain_operation_id(operation_id)
            .map_err(napi_error)?;
        let receipt = checkout
            .resume_live(
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await;
        let receipt = match receipt {
            Ok(receipt) => {
                if live_outcome_resolved(&receipt.value) {
                    checkout.clear_retained_operation(operation_id);
                }
                receipt
            }
            Err(error) => return Err(napi_error(error)),
        };
        live_mutation_result(receipt.value, receipt.work)
    }

    /// Safely advances to head and sparsely replays private mutations.
    #[napi]
    pub async fn rebase_head(&self, maximum_conflicts: NapiU32) -> Result<NativeRebaseResult> {
        let maximum_conflicts = maximum_conflicts.into_inner();

        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .rebase_head(
                maximum_conflicts,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        let (status, generation_id, conflict_count, truncated) = match receipt.value {
            RebaseDecision::Safe { generation } => (
                "safe",
                Some(Buffer::from(generation.digest().into_bytes().to_vec())),
                0,
                false,
            ),
            RebaseDecision::Conflicted {
                conflicts,
                truncated,
            } => (
                "conflicted",
                None,
                u32::try_from(conflicts.len()).unwrap_or(u32::MAX),
                truncated,
            ),
        };
        Ok(NativeRebaseResult {
            status: status.to_owned(),
            generation_id,
            conflict_count,
            truncated,
            work_json: work_json(&receipt.work)?,
        })
    }
}

pub(in crate::bindings) fn commit_id(bytes: &[u8]) -> Result<GitCommitId> {
    Ok(GitCommitId::from_bytes(fixed_32(
        bytes,
        "Git compatibility commit identity",
    )?))
}

pub(in crate::bindings) fn encode_created_mutation(
    receipt: &acyclic_fs::FsReceipt<FileId>,
) -> Result<NativeMutationResult> {
    Ok(NativeMutationResult {
        file_id: Some(Buffer::from(receipt.value.into_bytes().to_vec())),
        work_json: work_json(&receipt.work)?,
    })
}

pub(in crate::bindings) fn mutation_result(
    work: acyclic_fs::WorkCounters,
) -> Result<NativeMutationResult> {
    Ok(NativeMutationResult {
        file_id: None,
        work_json: work_json(&work)?,
    })
}

pub(in crate::bindings) fn commit_result(
    outcome: CheckoutCommitOutcome,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeCommitResult> {
    let (status, generation_id, epoch, sequence, fingerprint) = match outcome {
        CheckoutCommitOutcome::Committed {
            generation_id,
            head,
        } => (
            "committed",
            Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
            Some(bigint(head.epoch.get())),
            Some(bigint(head.sequence.get())),
            None,
        ),
        CheckoutCommitOutcome::AlreadyCommitted {
            generation_id,
            head,
        } => (
            "already-committed",
            Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
            Some(bigint(head.epoch.get())),
            Some(bigint(head.sequence.get())),
            None,
        ),
        CheckoutCommitOutcome::Conflict { actual } => (
            "conflict",
            None,
            Some(bigint(actual.epoch.get())),
            Some(bigint(actual.sequence.get())),
            None,
        ),
        CheckoutCommitOutcome::Fenced { actual_epoch } => {
            ("fenced", None, Some(bigint(actual_epoch.get())), None, None)
        }
        CheckoutCommitOutcome::IdempotencyConflict {
            committed_fingerprint,
        } => (
            "idempotency-conflict",
            None,
            None,
            None,
            Some(Buffer::from(committed_fingerprint.into_bytes().to_vec())),
        ),
    };
    Ok(NativeCommitResult {
        status: status.to_owned(),
        generation_id,
        epoch,
        sequence,
        committed_fingerprint: fingerprint,
        work_json: work_json(&work)?,
    })
}

pub(in crate::bindings) fn live_mutation_result(
    outcome: LiveMutationOutcome,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeLiveMutationResult> {
    let (status, generation_id, epoch, sequence, conflict_count, truncated, fingerprint) =
        match outcome {
            LiveMutationOutcome::Committed {
                generation_id,
                head,
            } => (
                "committed",
                Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
                Some(bigint(head.epoch.get())),
                Some(bigint(head.sequence.get())),
                0,
                false,
                None,
            ),
            LiveMutationOutcome::AlreadyCommitted {
                generation_id,
                head,
            } => (
                "already-committed",
                Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
                Some(bigint(head.epoch.get())),
                Some(bigint(head.sequence.get())),
                0,
                false,
                None,
            ),
            LiveMutationOutcome::Conflicted {
                conflicts,
                truncated,
            } => (
                "conflicted",
                None,
                None,
                None,
                u32::try_from(conflicts.len()).unwrap_or(u32::MAX),
                truncated,
                None,
            ),
            LiveMutationOutcome::RetryLimit { actual } => (
                "retry-limit",
                None,
                Some(bigint(actual.epoch.get())),
                Some(bigint(actual.sequence.get())),
                0,
                false,
                None,
            ),
            LiveMutationOutcome::Fenced { actual_epoch } => (
                "fenced",
                None,
                Some(bigint(actual_epoch.get())),
                None,
                0,
                false,
                None,
            ),
            LiveMutationOutcome::IdempotencyConflict {
                committed_fingerprint,
            } => (
                "idempotency-conflict",
                None,
                None,
                None,
                0,
                false,
                Some(Buffer::from(committed_fingerprint.into_bytes().to_vec())),
            ),
        };
    Ok(NativeLiveMutationResult {
        status: status.to_owned(),
        generation_id,
        epoch,
        sequence,
        conflict_count,
        truncated,
        committed_fingerprint: fingerprint,
        work_json: work_json(&work)?,
    })
}

/// No-work callback for publication.
pub(in crate::bindings) fn publication_ready<E>(
    _: acyclic_fs::WorkBudget,
) -> std::future::Ready<acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, E>> {
    std::future::ready(Ok(acyclic_fs::OperationReceipt {
        value: (),
        work: acyclic_fs::WorkCounters::UNCHARGED,
    }))
}
