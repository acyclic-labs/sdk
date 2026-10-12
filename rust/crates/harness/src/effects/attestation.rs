use super::*;

impl EffectRegistry {
    pub(crate) async fn dispatch_state_and_attest(
        &self,
        issuer: &AuthorityIssuer,
        effect_id: EffectId,
        effect: &EffectState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectAttestation> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        if effect.status != EffectStatus::Dispatched {
            return Err(crate::effect_host::effect_error(
                custody.work(),
                crate::wire::ErrorCode::Conflict,
                "effect is not awaiting dispatch",
            ));
        }
        let attempt_id = effect.attempts.last().copied().ok_or_else(|| {
            crate::effect_host::effect_error(
                custody.work(),
                crate::wire::ErrorCode::Conflict,
                "effect has no durable dispatch attempt",
            )
        })?;
        let provider = match custody.work().as_deref_mut() {
            Some(io) => io.clone_text(&effect.provider)?,
            None => effect.provider.clone(),
        };
        custody.adopt(provider.capacity() as u64)?;
        let effect_kind = match custody.work().as_deref_mut() {
            Some(io) => io.clone_text(&effect.effect_kind)?,
            None => effect.effect_kind.clone(),
        };
        custody.adopt(effect_kind.capacity() as u64)?;
        let request = effect.request.copy(custody.work())?;
        custody.adopt(request.logical_owned_bytes()?)?;
        let dispatch = EffectDispatch {
            provider,
            effect_id,
            attempt_id,
            effect_kind,
            request,
            guarantee: effect.guarantee,
            request_digest: effect.request_digest,
        };
        self.dispatch_and_attest(issuer, effect_id, effect, dispatch, custody.work())
            .await
    }

    pub(crate) async fn dispatch_and_attest(
        &self,
        issuer: &AuthorityIssuer,
        effect_id: EffectId,
        effect: &EffectState,
        dispatch: EffectDispatch,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectAttestation> {
        let provider = self.resolve(effect, work)?;
        self.validate_dispatch(effect_id, effect, &dispatch, work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            let future = provider.dispatch_with_native_work(dispatch, native)?;
            let (observation, custody) = future.await?;
            return self
                .attest_native_observation(issuer, effect_id, effect, (observation, custody), work)
                .await;
        }
        if work.is_some() {
            return Err(Error::Unsupported(
                "effect observer has no original native ledger".into(),
            ));
        }
        let observation = provider.dispatch(dispatch).await?;
        self.attest_provider_observation(issuer, effect_id, effect, observation, work)
            .await
    }

    pub(crate) async fn reconcile_and_attest(
        &self,
        issuer: &AuthorityIssuer,
        effect_id: EffectId,
        effect: &EffectState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<EffectAttestation>> {
        let provider = self.resolve(effect, work)?;
        let attempt_id = effect.attempts.last().copied().ok_or_else(|| {
            crate::effect_host::effect_error(
                work,
                crate::wire::ErrorCode::Conflict,
                "effect has no durable dispatch attempt",
            )
        })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            let future = provider.reconcile_with_native_work(attempt_id, native)?;
            return match future.await? {
                Some((observation, custody)) => Ok(Some(
                    self.attest_native_observation(
                        issuer,
                        effect_id,
                        effect,
                        (observation, custody),
                        work,
                    )
                    .await?,
                )),
                None => Ok(None),
            };
        }
        if work.is_some() {
            return Err(Error::Unsupported(
                "effect observer has no original native ledger".into(),
            ));
        }
        match provider.reconcile(attempt_id).await? {
            Some(observation) => Ok(Some(
                self.attest_provider_observation(issuer, effect_id, effect, observation, work)
                    .await?,
            )),
            None => Ok(None),
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) async fn attest_native_observation(
        &self,
        issuer: &AuthorityIssuer,
        effect_id: EffectId,
        effect: &EffectState,
        incoming: (EffectObservation, acyclic_fs::NativeAllocation),
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectAttestation> {
        if !work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .is_some_and(|native| incoming.1.work().shares_ledger(native))
        {
            let (observation, custody) = incoming;
            drop(observation);
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: Some(custody),
            });
        }
        let provider_bytes = incoming.0.provider.capacity() as u64;
        let (observation, custody) = incoming;
        let result = self
            .attest_provider_observation(issuer, effect_id, effect, observation, work)
            .await;
        match result {
            Ok(attestation) => {
                let io = work.as_deref_mut().expect("original observer");
                io.absorb_native_custody(custody)?;
                io.release_owned(provider_bytes)?;
                Ok(attestation)
            }
            Err(error) => {
                drop(custody);
                Err(error)
            }
        }
    }

    pub(super) fn validate_dispatch(
        &self,
        effect_id: EffectId,
        effect: &EffectState,
        dispatch: &EffectDispatch,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if dispatch.provider != effect.provider
            || dispatch.effect_id != effect_id
            || effect.attempts.last() != Some(&dispatch.attempt_id)
            || dispatch.effect_kind != effect.effect_kind
            || dispatch.request != effect.request
            || dispatch.request_digest != effect.request_digest
            || dispatch.guarantee != effect.guarantee
        {
            return Err(crate::effect_host::effect_error(
                work,
                crate::wire::ErrorCode::Conflict,
                "effect dispatch does not match durable state",
            ));
        }
        Ok(())
    }

    pub(super) async fn attest_provider_observation(
        &self,
        issuer: &AuthorityIssuer,
        effect_id: EffectId,
        effect: &EffectState,
        observation: EffectObservation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectAttestation> {
        self.validate_observation(effect_id, effect, &observation, work)?;
        if let EffectStatus::Succeeded { result } = &observation.status {
            let resolver = self.result_resolver.as_ref().ok_or_else(|| {
                crate::effect_host::effect_error(
                    work,
                    crate::wire::ErrorCode::Unsupported,
                    "effect result content resolver is not bound",
                )
            })?;
            if work.is_some() {
                let mut content = crate::wire_codec::CodecQuota::new(work, 0)?;
                let (bytes, claim) = content
                    .work()
                    .as_deref_mut()
                    .expect("observer")
                    .read_content(resolver.as_ref(), result)?
                    .await?;
                content.adopt(claim)?;
                let (schema_bytes, claim) = content
                    .work()
                    .as_deref_mut()
                    .expect("observer")
                    .read_content(resolver.as_ref(), &effect.result_schema)?
                    .await?;
                content.adopt(claim)?;
                validate_result_bytes(
                    &effect.result_schema,
                    &schema_bytes,
                    result,
                    &bytes,
                    content.work(),
                )?;
            } else {
                let bytes = resolver
                    .read(result, result.descriptor().byte_length(), None)?
                    .await?;
                let schema_bytes = resolver
                    .read(
                        &effect.result_schema,
                        effect.result_schema.descriptor().byte_length(),
                        None,
                    )?
                    .await?;
                validate_result_bytes(&effect.result_schema, &schema_bytes, result, &bytes, work)?;
            }
        }
        issuer.attest_effect(
            effect_id,
            effect,
            observation.attempt_id,
            observation.status,
            work,
        )
    }
}
