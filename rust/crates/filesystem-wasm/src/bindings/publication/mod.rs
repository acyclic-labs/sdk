use super::*;

#[derive(Serialize, Tsify)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum BrowserTransactionRebaseStatus {
    Rebased,
    Conflicted,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserMutationResult {
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) file_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserCommitResult {
    #[tsify(
        type = "\"committed\" | \"already-committed\" | \"conflict\" | \"fenced\" | \"idempotency-conflict\""
    )]
    pub(in crate::bindings) status: &'static str,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) generation_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) epoch: Option<u64>,
    pub(in crate::bindings) sequence: Option<u64>,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) committed_fingerprint: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserRebaseResult {
    #[tsify(type = "\"safe\" | \"conflicted\"")]
    pub(in crate::bindings) status: &'static str,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) generation_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) conflict_count: u32,
    pub(in crate::bindings) truncated: bool,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserLiveMutationResult {
    #[tsify(
        type = "\"committed\" | \"already-committed\" | \"conflicted\" | \"retry-limit\" | \"fenced\" | \"idempotency-conflict\""
    )]
    pub(in crate::bindings) status: &'static str,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) generation_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) epoch: Option<u64>,
    pub(in crate::bindings) sequence: Option<u64>,
    pub(in crate::bindings) conflict_count: u32,
    pub(in crate::bindings) truncated: bool,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) committed_fingerprint: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[wasm_bindgen]
impl BrowserCheckout {
    /// Builds an immutable candidate generation without publishing authority.
    #[wasm_bindgen(js_name = checkpoint)]
    pub async fn checkpoint(&self) -> Result<Ts<BrowserCheckpointResult>, JsValue> {
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .checkpoint(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .await
            .map_err(js_error)?;
        encode_checkpoint_result(&receipt)
    }

    /// Explicitly advances a clean manual checkout to the authority head.
    #[wasm_bindgen(js_name = refreshHead)]
    pub async fn refresh_head(&mut self) -> Result<Ts<BrowserCheckpointResult>, JsValue> {
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .refresh_head(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .await
            .map_err(js_error)?;
        encode_checkpoint_result(&receipt)
    }

    /// Explicitly performs observation-safe synchronization for a live checkout.
    #[wasm_bindgen(js_name = refreshLive)]
    pub async fn refresh_live(&mut self) -> Result<Ts<BrowserCheckpointResult>, JsValue> {
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .refresh_live(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .await
            .map_err(js_error)?;
        encode_checkpoint_result(&receipt)
    }

    /// Checkpoints and conditionally publishes this private overlay.
    pub async fn commit(
        &mut self,
        operation_id: Vec<u8>,
        #[wasm_bindgen(unchecked_param_type = "BrowserOperationWindowLease | undefined")]
        lease: Option<JsValue>,
    ) -> Result<Ts<BrowserCommitResult>, JsValue> {
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let permit = browser_publication_permit(lease)?;
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .commit(
                operation_id,
                None,
                None,
                None,
                permit,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
                |_| async {
                    acyclic_fs::FsResult::Ok(acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::default(),
                    })
                },
            )
            .await
            .map_err(js_error)?;
        commit_value(receipt.value, receipt.work)
    }

    /// Applies and publishes one direct-live transaction with bounded safe retries.
    #[wasm_bindgen(js_name = mutateLive)]
    pub async fn mutate_live(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "TransactionOperation[]")] operations: JsValue,
        operation_id: Vec<u8>,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_attempts: JsValue,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_conflicts: JsValue,
    ) -> Result<Ts<BrowserLiveTransactionResult>, JsValue> {
        let maximum_conflicts = wasm_u32(&maximum_conflicts)?;
        let maximum_attempts = wasm_u32(&maximum_attempts)?;

        let authored = decode_authored_transactions(
            operations,
            self.engine.volume_config().profile,
            self.engine.volume_config().limits,
        )?;
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .apply_authored_live(
                authored,
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        authored_live_mutation_value(receipt.value, receipt.work)
    }

    /// Resumes an unresolved direct-live transaction with the same operation identity.
    #[wasm_bindgen(js_name = resumeLive)]
    pub async fn resume_live(
        &mut self,
        operation_id: Vec<u8>,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_attempts: JsValue,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_conflicts: JsValue,
    ) -> Result<Ts<BrowserLiveMutationResult>, JsValue> {
        let maximum_conflicts = wasm_u32(&maximum_conflicts)?;
        let maximum_attempts = wasm_u32(&maximum_attempts)?;

        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .resume_live(
                operation_id,
                maximum_attempts,
                maximum_conflicts,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        live_mutation_value(receipt.value, receipt.work).and_then(ts)
    }

    /// Safely advances to head and sparsely replays private mutations.
    #[wasm_bindgen(js_name = rebaseHead)]
    pub async fn rebase_head(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_conflicts: JsValue,
    ) -> Result<Ts<BrowserRebaseResult>, JsValue> {
        let maximum_conflicts = wasm_u32(&maximum_conflicts)?;

        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .rebase_head(
                maximum_conflicts,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        let (status, generation_id, conflict_count, truncated) = match receipt.value {
            RebaseDecision::Safe { generation } => (
                "safe",
                Some(generation.digest().into_bytes().to_vec()),
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
        ts(BrowserRebaseResult {
            status,
            generation_id: generation_id.map(Into::into),
            conflict_count,
            truncated,
            work: browser_work(receipt.work),
        })
    }
}

pub(in crate::bindings) fn mutation_value(
    work: acyclic_fs::WorkCounters,
) -> Result<Ts<BrowserMutationResult>, JsValue> {
    ts(BrowserMutationResult {
        file_id: None,
        work: browser_work(work),
    })
}

pub(in crate::bindings) fn mutation_created(
    receipt: &acyclic_fs::OperationReceipt<FileId>,
) -> Result<Ts<BrowserMutationResult>, JsValue> {
    ts(BrowserMutationResult {
        file_id: Some(receipt.value.into_bytes().to_vec().into()),
        work: browser_work(receipt.work),
    })
}

pub(in crate::bindings) fn commit_value(
    outcome: LivePublicationObservation,
    work: acyclic_fs::WorkCounters,
) -> Result<Ts<BrowserCommitResult>, JsValue> {
    let (status, generation_id, epoch, sequence, fingerprint) = match outcome {
        LivePublicationObservation::Committed {
            generation_id,
            head,
        } => (
            "committed",
            Some(generation_id.digest().into_bytes().to_vec()),
            Some(head.epoch.get()),
            Some(head.sequence.get()),
            None,
        ),
        LivePublicationObservation::AlreadyCommitted {
            generation_id,
            head,
        } => (
            "already-committed",
            Some(generation_id.digest().into_bytes().to_vec()),
            Some(head.epoch.get()),
            Some(head.sequence.get()),
            None,
        ),
        LivePublicationObservation::Conflict { actual } => (
            "conflict",
            None,
            Some(actual.epoch.get()),
            Some(actual.sequence.get()),
            None,
        ),
        LivePublicationObservation::Fenced { actual_epoch } => {
            ("fenced", None, Some(actual_epoch.get()), None, None)
        }
        LivePublicationObservation::IdempotencyConflict {
            committed_fingerprint,
        } => (
            "idempotency-conflict",
            None,
            None,
            None,
            Some(committed_fingerprint.into_bytes().to_vec()),
        ),
    };
    ts(BrowserCommitResult {
        status,
        generation_id: generation_id.map(Into::into),
        epoch,
        sequence,
        committed_fingerprint: fingerprint.map(Into::into),
        work: browser_work(work),
    })
}

pub(in crate::bindings) fn live_mutation_value(
    outcome: LiveMutationOutcome,
    work: acyclic_fs::WorkCounters,
) -> Result<BrowserLiveMutationResult, JsValue> {
    let (status, generation_id, epoch, sequence, conflict_count, truncated, fingerprint) =
        match outcome {
            LiveMutationOutcome::Committed {
                generation_id,
                head,
            } => (
                "committed",
                Some(generation_id.digest().into_bytes().to_vec()),
                Some(head.epoch.get()),
                Some(head.sequence.get()),
                0,
                false,
                None,
            ),
            LiveMutationOutcome::AlreadyCommitted {
                generation_id,
                head,
            } => (
                "already-committed",
                Some(generation_id.digest().into_bytes().to_vec()),
                Some(head.epoch.get()),
                Some(head.sequence.get()),
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
                Some(actual.epoch.get()),
                Some(actual.sequence.get()),
                0,
                false,
                None,
            ),
            LiveMutationOutcome::Fenced { actual_epoch } => (
                "fenced",
                None,
                Some(actual_epoch.get()),
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
                Some(committed_fingerprint.into_bytes().to_vec()),
            ),
        };
    Ok(BrowserLiveMutationResult {
        status,
        generation_id: generation_id.map(Into::into),
        epoch,
        sequence,
        conflict_count,
        truncated,
        committed_fingerprint: fingerprint.map(Into::into),
        work: browser_work(work),
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
