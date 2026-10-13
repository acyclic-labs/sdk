use super::*;

/// Successful authored mutation with exact work evidence.
#[napi(object)]
pub struct NativeMutationResult {
    pub file_id: Option<Buffer>,
    pub work_json: String,
}

/// Conditional publication outcome with exact authority facts.
#[napi(object)]
pub struct NativeCommitResult {
    pub status: String,
    pub generation_id: Option<Buffer>,
    pub epoch: Option<BigInt>,
    pub sequence: Option<BigInt>,
    pub committed_fingerprint: Option<Buffer>,
    pub work_json: String,
}

/// Observation-safe head-rebase outcome.
#[napi(object)]
pub struct NativeRebaseResult {
    pub status: String,
    pub generation_id: Option<Buffer>,
    pub conflict_count: u32,
    pub truncated: bool,
    pub work_json: String,
}

/// Terminal result of publishing one explicitly staged direct-live transaction.
#[napi(object)]
pub struct NativeLiveMutationResult {
    pub status: String,
    pub generation_id: Option<Buffer>,
    pub epoch: Option<BigInt>,
    pub sequence: Option<BigInt>,
    pub conflict_count: u32,
    pub truncated: bool,
    pub committed_fingerprint: Option<Buffer>,
    pub work_json: String,
}

/// Observation-safe retained transaction rebase result.
#[napi(object)]
pub struct NativeTransactionRebaseResult {
    pub status: String,
    pub generation_id: Option<Buffer>,
    pub conflicts: Vec<NativeTransactionConflict>,
    pub truncated: bool,
}

#[napi]
impl NativeCheckout {
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

    #[napi]
    pub async fn commit(&self, operation_id: Buffer) -> Result<NativeCommitResult> {
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
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
                acyclic_fs::PublicationPermit::Unrestricted,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
                |_| async {
                    acyclic_fs::FsResult::Ok(acyclic_fs::OperationReceipt {
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

    #[napi]
    pub async fn rebase_head(&self, maximum_conflicts: NapiU32) -> Result<NativeRebaseResult> {
        let maximum_conflicts = maximum_conflicts.into_inner();

        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout_call!(self, checkout, rebase_head(maximum_conflicts,))?;
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

pub(in crate::bindings) fn encode_created_mutation(
    receipt: &acyclic_fs::OperationReceipt<FileId>,
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
    outcome: LivePublicationObservation,
    work: acyclic_fs::WorkCounters,
) -> Result<NativeCommitResult> {
    let (status, generation_id, epoch, sequence, fingerprint) = match outcome {
        LivePublicationObservation::Committed {
            generation_id,
            head,
        } => (
            "committed",
            Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
            Some(bigint(head.epoch.get())),
            Some(bigint(head.sequence.get())),
            None,
        ),
        LivePublicationObservation::AlreadyCommitted {
            generation_id,
            head,
        } => (
            "already-committed",
            Some(Buffer::from(generation_id.digest().into_bytes().to_vec())),
            Some(bigint(head.epoch.get())),
            Some(bigint(head.sequence.get())),
            None,
        ),
        LivePublicationObservation::Conflict { actual } => (
            "conflict",
            None,
            Some(bigint(actual.epoch.get())),
            Some(bigint(actual.sequence.get())),
            None,
        ),
        LivePublicationObservation::Fenced { actual_epoch } => {
            ("fenced", None, Some(bigint(actual_epoch.get())), None, None)
        }
        LivePublicationObservation::IdempotencyConflict {
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
