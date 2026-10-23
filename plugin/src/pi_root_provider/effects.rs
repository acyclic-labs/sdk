//! Original root effect authentication, dispatch and reconciliation.
use super::*;
use acyclic_harness::filesystem::PreparationIo;

impl PiRootProvider {
    pub(super) fn native_failure(reason: &'static str) -> Error {
        Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason,
        }
    }

    pub(super) fn validate_attachment(&self, input: &(SourceReference, [u8; 16])) -> Result<()> {
        if input.0.identity == [0; 16] || input.1 == [0; 16] {
            return Err(Self::native_failure(
                "root attachment has an empty original source identity",
            ));
        }
        Ok(())
    }

    pub(super) fn check_native_ledger(&self, work: &NativeMaterializationWork) -> Result<()> {
        if !self.original.shares_ledger(work) {
            return Err(Self::native_failure(
                "root allocation must use its original request ledger",
            ));
        }
        work.admit(WorkCounters::UNCHARGED).map_err(Error::from)
    }

    pub(super) fn artifact_with_native_work(
        &self,
        effect: EffectId,
        attempt: EffectAttemptId,
        name: &str,
        work: &NativeMaterializationWork,
    ) -> Result<NativeInputCoordinates> {
        let display_name = work.display(&format_args!("{name}.json"))?;
        let path = work.display(&format_args!(
            "{ROOT_INPUT_DIRECTORY}/allocation/{effect}/{attempt}/{}",
            display_name.0
        ))?;
        if path.0.len() > self.owner.policy().limits.path_bytes {
            return Err(Error::NativeBoundary {
                code: ErrorCode::Invalid,
                reason: "root allocation artifact exceeds original path limit",
            });
        }
        let (key, custody) = work.display(&format_args!(
            "acyclic.pi/root-allocation/{}/{}/{effect}/{attempt}/{name}",
            self.owner.policy().root_task,
            self.preparation.command.operation_id
        ))?;
        Ok(NativeInputCoordinates {
            path,
            display_name,
            key: (IdempotencyKey::new(key)?, custody),
        })
    }

    pub(super) async fn original_artifact_with_native_work(
        &self,
        effect: EffectId,
        attempt: EffectAttemptId,
        name: &str,
        work: &NativeMaterializationWork,
    ) -> Result<Option<(FileRef, NativeAllocation)>> {
        let grant = super::inputs::write_grant(&self.owner, work)?;
        observe_pi_input_at(
            &self.owner,
            &self.filesystem,
            self.artifact_with_native_work(effect, attempt, name, work)?,
            "application/json",
            &grant.0,
            work,
        )
        .await
    }

    pub(super) async fn stage_with_native_work(
        &self,
        effect: EffectId,
        attempt: EffectAttemptId,
        name: &str,
        value: &(impl Serialize + Sync),
        work: &NativeMaterializationWork,
    ) -> Result<(FileRef, NativeAllocation)> {
        publish_pi_input_with_native_coordinates(
            &self.owner,
            &self.filesystem,
            value,
            "application/json",
            self.artifact_with_native_work(effect, attempt, name, work)?,
            work,
        )
        .await
    }

    pub(super) async fn read_with_native_work<T: DeserializeOwned + Serialize>(
        &self,
        reference: &FileRef,
        work: &NativeMaterializationWork,
    ) -> Result<(T, NativeAllocation)> {
        read_root_artifact_with_native_work(&self.owner, &self.preparation.context, reference, work)
            .await
    }

    pub(super) async fn authenticate_with_native_work(
        &self,
        attempt: EffectAttemptId,
        require_success: bool,
        work: &NativeMaterializationWork,
    ) -> Result<(EffectState, NativeAllocation)> {
        self.check_native_ledger(work)?;
        let effect = self.effect_id_with_native_work(work)?;
        let verifier = self.owner.effect_verifier_with_native_work(work)?;
        let mut reader_io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        let reader = HistoryReader::new(
            &self.stream,
            verifier.0.audience(),
            &verifier.0,
            &mut Some(&mut reader_io),
        )?;
        let cursor = reader.pin(0, Some(&mut reader_io)).await?;
        let mut io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        let state = reader
            .effect(
                &cursor,
                effect,
                HistoryReadLimits {
                    maximum_events: self
                        .owner
                        .policy()
                        .limits
                        .context_messages
                        .try_into()
                        .map_err(|_| {
                            Self::native_failure("root history event limit exceeds the SDK count")
                        })?,
                    maximum_bytes: self.maximum(),
                },
                Some(&mut io),
            )
            .await?
            .ok_or_else(|| Self::native_failure("original root effect is absent"))?
            .0;
        let state = (state, io.into_custody()?);
        let plan = &self.preparation.plan;
        if state.0.provider != ROOT_ALLOCATION_PROVIDER
            || state.0.effect_kind != ROOT_ALLOCATION_EFFECT
            || state.0.guarantee != EffectGuarantee::AtMostOnce
            || state.0.request.as_ref() != &plan.request
            || state.0.result_schema != plan.result_schema
            || state.0.attempts.as_slice() != [attempt]
            || !matches!(
                state.0.status,
                EffectStatus::Dispatched
                    | EffectStatus::Indeterminate
                    | EffectStatus::Succeeded { .. }
            )
            || (require_success && !matches!(state.0.status, EffectStatus::Succeeded { .. }))
        {
            return Err(Self::native_failure(
                "root effect differs from its original request, schema, provider or attempt",
            ));
        }
        // The preparation loader authenticates its immutable plan, request and schema once.
        if let EffectStatus::Succeeded { result } = &state.0.status {
            let canonical = self
                .original_artifact_with_native_work(effect, attempt, "result", work)
                .await?;
            if canonical.as_ref().map(|reference| &reference.0) != Some(result) {
                return Err(Self::native_failure(
                    "root success differs from its original immutable output operation",
                ));
            }
        }
        Ok(state)
    }

    pub(super) fn execution_owner(&self) -> Result<&TaskJournalOwner<LocalStream>> {
        self.journal_owner.as_deref().ok_or_else(|| {
            Self::native_failure("archived root observer cannot dispatch or resume creation")
        })
    }

    pub(crate) async fn take_created_handles(&self) -> BTreeMap<String, PiRootHandle> {
        std::mem::take(&mut self.state.lock().await.handles)
    }

    pub(crate) fn effect_id_with_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<EffectId> {
        acyclic_harness::effect_host::task_effect_id(
            self.owner.policy().root_task,
            self.preparation.command.operation_id,
            &mut Some(&mut &PreparationIo::from_native_work(work)?),
        )
    }

    pub(super) fn maximum(&self) -> u64 {
        self.owner
            .policy()
            .maximum_payload_bytes
            .min(crate::MAXIMUM_ADAPTER_STATE_BYTES)
    }

    pub(super) fn creation_operation_with_native_work(
        &self,
        effect: EffectId,
        attempt: EffectAttemptId,
        resource: &str,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_fs::OperationId> {
        let digest = digest_pi_value(
            &(
                "acyclic.pi/root-resource/v1",
                self.owner.policy().root_task,
                self.preparation.command.operation_id,
                effect,
                attempt,
                resource,
            ),
            self.maximum(),
            work,
        )?;
        let mut id = [0; 16];
        id.copy_from_slice(&digest.as_bytes()[..16]);
        Ok(acyclic_fs::OperationId::from_bytes(id))
    }

    #[cfg(test)]
    pub(super) fn test_fault_native(&self, phase: &'static str) -> Result<()> {
        let code = match phase {
            "after-root-creation" => 1,
            "before-result-stage" => 2,
            "before-attachments-stage" => 3,
            "after-result-stage" => 4,
            "after-conversation-bind" => 5,
            _ => unreachable!(),
        };
        if self
            .fault
            .compare_exchange(
                code,
                0,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
        {
            return Err(Error::NativeBoundary {
                code: ErrorCode::Storage,
                reason: phase,
            });
        }
        Ok(())
    }
}

impl EffectProvider for PiRootProvider {
    fn id(&self) -> &str {
        ROOT_ALLOCATION_PROVIDER
    }
    fn guarantees(&self, kind: &str) -> BTreeSet<EffectGuarantee> {
        if kind == ROOT_ALLOCATION_EFFECT {
            [EffectGuarantee::AtMostOnce].into()
        } else {
            BTreeSet::new()
        }
    }
    fn linearizable_reconciliation(&self) -> bool {
        false
    }
    fn dispatch<'a>(
        &'a self,
        _request: EffectDispatch,
    ) -> BoxProviderFuture<'a, Result<EffectObservation>> {
        Box::pin(async {
            Err(Error::NativeBoundary {
                code: ErrorCode::Unsupported,
                reason: "root allocation requires the funded native effect dispatch boundary",
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _attempt: EffectAttemptId,
    ) -> BoxProviderFuture<'a, Result<Option<EffectObservation>>> {
        Box::pin(async {
            Err(Error::NativeBoundary {
                code: ErrorCode::Unsupported,
                reason: "root allocation requires the funded native effect reconciliation boundary",
            })
        })
    }
    fn dispatch_with_native_work<'a>(
        &'a self,
        request: EffectDispatch,
        work: &'a NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, (EffectObservation, NativeAllocation)>>
    {
        self.check_native_ledger(work)?;
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            // The root effect is its own invocation on the session ledger.
            let window = work.invocation_window()?;
            let work = &window;
            let mut state = self.state.lock().await;
            let original = self
                .authenticate_with_native_work(request.attempt_id, false, work)
                .await?;
            let effect = self.effect_id_with_native_work(work)?;
            if request.effect_id != effect
                || request.provider != original.0.provider
                || request.effect_kind != original.0.effect_kind
                || &request.request != original.0.request.as_ref()
                || request.guarantee != original.0.guarantee
                || request.request_digest != original.0.request_digest
            {
                return Err(Self::native_failure(
                    "root dispatch differs from original durable attempt",
                ));
            }
            self.execution_owner()?
                .verify(
                    false,
                    &mut Some(&mut &PreparationIo::from_native_work(work)?),
                )
                .await?;
            if let Some(observation) = self
                .observe_result_with_native_work(
                    effect,
                    request.attempt_id,
                    &original.0,
                    false,
                    work,
                )
                .await?
            {
                state.dispatched = true;
                return Ok(observation);
            }
            if state.dispatched {
                return Err(Self::native_failure(
                    "original root dispatch remains indeterminate",
                ));
            }
            state.dispatched = true;
            self.allocate_with_native_work(
                effect,
                request.attempt_id,
                &original.0,
                &mut state,
                work,
            )
            .await
        })
    }
    fn reconcile_with_native_work<'a>(
        &'a self,
        attempt: EffectAttemptId,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        acyclic_harness::NativeFutureBundle<'a, Option<(EffectObservation, NativeAllocation)>>,
    > {
        self.check_native_ledger(work)?;
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            // The root effect is its own invocation on the session ledger.
            let window = work.invocation_window()?;
            let work = &window;
            let mut state = self.state.lock().await;
            self.execution_owner()?
                .verify(
                    true,
                    &mut Some(&mut &PreparationIo::from_native_work(work)?),
                )
                .await?;
            let original = self
                .authenticate_with_native_work(attempt, false, work)
                .await?;
            let effect = self.effect_id_with_native_work(work)?;
            if let Some(observation) = self
                .observe_result_with_native_work(effect, attempt, &original.0, true, work)
                .await?
            {
                return Ok(Some(observation));
            }
            match self
                .execution_owner()?
                .verify(
                    false,
                    &mut Some(&mut &PreparationIo::from_native_work(work)?),
                )
                .await
            {
                Ok(()) => {
                    state.dispatched = true;
                    self.allocate_with_native_work(effect, attempt, &original.0, &mut state, work)
                        .await
                        .map(Some)
                }
                Err(
                    Error::Conflict(_)
                    | Error::NativeBoundary {
                        code: ErrorCode::Conflict,
                        ..
                    },
                ) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }
    fn supports_guarantee_with_native_work(
        &self,
        kind: &str,
        guarantee: EffectGuarantee,
        work: &NativeMaterializationWork,
    ) -> Result<bool> {
        self.check_native_ledger(work)?;
        Ok(kind == ROOT_ALLOCATION_EFFECT && guarantee == EffectGuarantee::AtMostOnce)
    }
}
