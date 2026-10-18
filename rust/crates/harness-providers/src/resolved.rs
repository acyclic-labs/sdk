use crate::{HttpTransport, ProviderSpec, invalid};
use acyclic_harness::{
    Result,
    context::{ModelContextCapacity, ModelTokenCount},
    model::{Model, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
};
use acyclic_stream::BoxProviderStream;
use std::sync::Arc;

/// Exact model selection and its provider, ready for a harness execution scope.
pub struct ResolvedModel {
    pub(crate) files: crate::files::Files,
    pub(crate) estimator: crate::accounting::TokenEstimator,
    /// Immutable harness model identity.
    pub(crate) model: Model,
    pub(crate) config: Arc<ProviderSpec>,
    pub(crate) capacity: ModelContextCapacity,
    pub(crate) key: Option<String>,
    pub(crate) transport: Arc<dyn HttpTransport>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) codex_count: crate::codex_count::Counter,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) codex_login: Option<Arc<crate::CodexLogin>>,
}

impl ResolvedModel {
    /// Binds the caller-authorized filesystem reader; canonical requests retain file references.
    pub fn with_content_reader(
        mut self,
        reader: Arc<dyn acyclic_harness::conversation::ContentResidencyVerifier>,
    ) -> Self {
        self.files.reader = Some(reader);
        self
    }

    /// Borrows the immutable identity used to bind this provider into the harness.
    pub fn model(&self) -> &Model {
        &self.model
    }
    pub(crate) fn reasoning_origin(
        &self,
        key: Option<&str>,
        account: Option<&str>,
    ) -> acyclic_harness::model::ReasoningOrigin {
        let family = crate::stream::family(self.config.adapter);
        let mut scope = blake3::Hasher::new();
        scope.update(b"harness-reasoning-scope-v1\0");
        for value in [
            family,
            self.config.base_url.as_str(),
            account.or(key).unwrap_or(""),
        ] {
            scope.update(&(value.len() as u64).to_be_bytes());
            scope.update(value.as_bytes());
        }
        acyclic_harness::model::ReasoningOrigin {
            provider: family.into(),
            model: self.model.name.clone(),
            credential_scope: Some(*scope.finalize().as_bytes()),
        }
    }

    pub(crate) fn observe(
        &self,
        request: &PreparedModelRequest,
        input_bytes: usize,
        metadata: &serde_json::Value,
    ) -> Result<()> {
        self.estimator.observe(input_bytes, metadata);
        #[cfg(not(target_arch = "wasm32"))]
        if self.codex_login.is_some() {
            self.codex_count.observe(request, metadata, |message| {
                crate::images::tokens(self, message)
            })?;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = request;
        Ok(())
    }

    pub(crate) fn context_output(&self) -> u32 {
        self.capacity.output_tokens
    }
    pub(crate) fn validate(&self, model: &Model) -> Result<()> {
        if model != &self.model {
            return Err(invalid("request model differs from resolved selection"));
        }
        Ok(())
    }
}

impl ModelProvider for ResolvedModel {
    fn image_input(&self, model: &Model) -> Result<Option<acyclic_harness::model::ImageInput>> {
        self.validate(model)?;
        Ok(self.config.images.get(&model.name).cloned())
    }

    fn bind_content(
        &self,
        reader: Arc<dyn acyclic_harness::conversation::ContentResidencyVerifier>,
        maximum_bytes: u64,
    ) -> Option<Arc<dyn ModelProvider>> {
        Some(Arc::new(Self {
            files: crate::files::Files {
                reader: Some(reader),
                maximum_bytes: maximum_bytes.min(self.files.maximum_bytes),
            },
            estimator: crate::accounting::TokenEstimator::new(self.config.adapter),
            model: self.model.clone(),
            config: self.config.clone(),
            capacity: self.capacity,
            key: self.key.clone(),
            transport: self.transport.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            codex_count: Default::default(),
            #[cfg(not(target_arch = "wasm32"))]
            codex_login: self.codex_login.clone(),
        }))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_model(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        self.transport.supports_native(work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn context_capacity_with_native_work(
        &self,
        model: &Model,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<ModelContextCapacity> {
        work.remaining()?;
        if model != &self.model {
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "request model differs from resolved selection",
            });
        }
        Ok(self.capacity)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn count_tokens_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_harness::NativeFutureBundle<'a, (ModelTokenCount, acyclic_fs::NativeAllocation)>,
    > {
        self.context_capacity_with_native_work(&request.request().model, work)?;
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let cancelled = work.cancellation_wait()?;
            let exchange = crate::counting::count(self, request, Some(work));
            futures::pin_mut!(cancelled, exchange);
            match futures::future::select(cancelled, exchange).await {
                futures::future::Either::Left(_) => {
                    Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into())
                }
                futures::future::Either::Right((result, _)) => {
                    let (count, custody) = result?;
                    Ok((
                        count,
                        custody.ok_or(acyclic_harness::Error::NativeBoundary {
                            code: acyclic_harness::wire::ErrorCode::Invalid,
                            reason: "verified count lost native custody",
                        })?,
                    ))
                }
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn generate_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_harness::NativeFutureBundle<'a, acyclic_harness::model::NativeModelStream<'a>>,
    > {
        crate::native::generate(self, request, dispatch, work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn reconcile_with_native_work<'a>(
        &'a self,
        _: ModelDispatch,
        _: &'a [ModelEvent],
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_harness::NativeFutureBundle<
            'a,
            (Option<Vec<ModelEvent>>, acyclic_fs::NativeAllocation),
        >,
    > {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            Ok((None, work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?))
        })
    }

    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        self.validate(model)?;
        Ok(self.capacity)
    }

    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
            crate::counting::count(self, request, None)
                .await
                .map(|(count, _)| count)
        })
    }

    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, Result<ModelEvent>> {
        crate::dispatch::generate(self, request, dispatch)
    }
}
