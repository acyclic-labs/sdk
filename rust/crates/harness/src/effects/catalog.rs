use super::*;

/// Explicit registry; no ambient or process-global provider catalog exists.
#[derive(Default)]
pub struct EffectRegistry {
    pub(super) providers: BTreeMap<String, Arc<dyn EffectProvider>>,
    pub(super) result_resolver: Option<Arc<dyn ContentResidencyVerifier>>,
    // Payload fields drop before the original ledger's custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_metadata: Option<acyclic_fs::NativeAllocation>,
}

impl Clone for EffectRegistry {
    fn clone(&self) -> Self {
        Self {
            providers: self.providers.clone(),
            result_resolver: self.result_resolver.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata: None,
        }
    }
}

impl EffectRegistry {
    /// Creates a catalog on optional original work.
    pub fn new(work: crate::NativeWork<'_>) -> Result<Self> {
        let registry = Self::default();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            return Ok(Self {
                native_metadata: Some(
                    work.claim(
                        std::mem::size_of::<Self>() as u64,
                        0,
                        acyclic_fs::WorkCounters::UNCHARGED,
                    )
                    .map_err(|cause| Error::NativeWork {
                        cause,
                        custody: None,
                    })?,
                ),
                ..registry
            });
        }
        let _ = work;
        Ok(registry)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn require_native_metadata(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if self
            .native_metadata
            .as_ref()
            .is_some_and(|guard| guard.work().shares_ledger(work))
        {
            Ok(())
        } else {
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "effect catalog does not retain the original native metadata",
            })
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Copies catalog metadata on the original ledger.
    pub fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        use crate::store::history::HistoryIo as _;
        let mut cloned = Self::new(Some(work))?;
        for (source, provider) in &self.providers {
            let mut io =
                crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
            let slot = std::mem::size_of::<(String, Arc<dyn EffectProvider>)>() as u64;
            io.admit_owned(slot)?;
            io.retain_owned(slot)?;
            let mut id = String::new();
            io.reserve_text(&mut id, source.len())?;
            io.admit_copy(source.len() as u64)?;
            id.push_str(source);
            io.copy_bytes(source.len() as u64)?;
            let mut custody = io.into_custody();
            cloned
                .native_metadata
                .as_mut()
                .expect("native catalog constructor")
                .absorb(&mut custody)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            cloned.providers.insert(id, Arc::clone(provider));
        }
        cloned.result_resolver = self.result_resolver.clone();
        Ok(cloned)
    }

    /// Sets the owner-mediated result reader.
    #[must_use]
    pub fn with_result_resolver(mut self, resolver: Arc<dyn ContentResidencyVerifier>) -> Self {
        self.result_resolver = Some(resolver);
        self
    }

    pub(crate) fn resolve(
        &self,
        effect: &EffectState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<&Arc<dyn EffectProvider>> {
        let provider_name = &effect.provider;
        let effect_kind = &effect.effect_kind;
        let guarantee = effect.guarantee;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            self.require_native_metadata(native)?;
            native
                .charge(acyclic_fs::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            let provider =
                self.providers
                    .get(provider_name)
                    .ok_or_else(|| Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "effect provider is not registered",
                    })?;
            if !provider.supports_guarantee_with_native_work(effect_kind, guarantee, native)? {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "effect provider does not support the pinned guarantee",
                });
            }
            if guarantee == EffectGuarantee::ExactlyOnce && !provider.linearizable_reconciliation()
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "exactly-once requires linearizable provider reconciliation",
                });
            }
            return Ok(provider);
        }
        if work.is_some() {
            return Err(Error::Unsupported(
                "effect observer has no original native ledger".into(),
            ));
        }
        let provider = self.providers.get(provider_name).ok_or_else(|| {
            crate::effect_host::effect_error(
                work,
                crate::wire::ErrorCode::Unsupported,
                "effect provider is not registered",
            )
        })?;
        if !provider.guarantees(effect_kind).contains(&guarantee) {
            return Err(Error::Unsupported(format!(
                "provider {provider_name} does not support {guarantee:?} for {effect_kind}"
            )));
        }
        if guarantee == EffectGuarantee::ExactlyOnce && !provider.linearizable_reconciliation() {
            return Err(Error::Unsupported(
                "exactly-once requires linearizable provider reconciliation".into(),
            ));
        }
        Ok(provider)
    }

    pub(crate) fn validate_observation(
        &self,
        effect_id: EffectId,
        effect: &EffectState,
        observation: &EffectObservation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.resolve(effect, work)?;
        if observation.provider != effect.provider
            || observation.effect_id != effect_id
            || effect.attempts.last() != Some(&observation.attempt_id)
            || observation.request_digest != effect.request_digest
            || observation.guarantee != effect.guarantee
            || !observation.status.is_terminal()
        {
            return Err(crate::effect_host::effect_error(
                work,
                crate::wire::ErrorCode::Conflict,
                "effect observation does not match durable dispatch",
            ));
        }
        Ok(())
    }

    /// Registers one provider on the catalog's original ledger.
    pub fn register(
        &mut self,
        provider: Arc<dyn EffectProvider>,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let inherited = if work.is_none() {
            self.native_metadata
                .as_ref()
                .map(|guard| guard.work().clone())
        } else {
            None
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let work = work.or(inherited.as_ref());
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut owned_io = if let Some(work) = work {
            self.require_native_metadata(work)?;
            let io = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
            use crate::store::history::HistoryIo as _;
            io.check_cancellation()?;
            Some(io)
        } else {
            None
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut observed = owned_io
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut observed = None;
        let source = provider.id();
        if source.trim().is_empty() {
            return Err(crate::effect_host::effect_error(
                &mut observed,
                crate::wire::ErrorCode::Invalid,
                "effect provider identity is empty",
            ));
        }
        if self.providers.contains_key(source) {
            return Err(crate::effect_host::effect_error(
                &mut observed,
                crate::wire::ErrorCode::Conflict,
                "effect provider is already registered",
            ));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let id = if let Some(mut io) = owned_io {
            use crate::store::history::HistoryIo as _;
            let slot = std::mem::size_of::<(String, Arc<dyn EffectProvider>)>() as u64;
            io.admit_owned(slot)?;
            io.retain_owned(slot)?;
            let mut id = String::new();
            io.reserve_text(&mut id, source.len())?;
            io.admit_copy(source.len() as u64)?;
            id.push_str(source);
            io.copy_bytes(source.len() as u64)?;
            let mut custody = io.into_custody();
            self.native_metadata
                .as_mut()
                .expect("validated native catalog")
                .absorb(&mut custody)?;
            id
        } else {
            source.to_owned()
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let id = source.to_owned();
        let _ = work;
        self.providers.insert(id, provider);
        Ok(())
    }
}
