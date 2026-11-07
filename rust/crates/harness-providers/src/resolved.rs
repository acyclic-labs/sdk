use crate::{Adapter, HttpTransport, ProviderSpec, invalid};
use acyclic_harness_core::{
    Result,
    context::{ModelContextCapacity, ModelTokenCount},
    model::{Model, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
};
use std::sync::Arc;

/// One selection's calibration and authoritative usage anchor. Reader-bound
/// copies retain this owner together, without separate allocation/refcounts.
pub(crate) struct Accounting {
    pub(crate) estimator: crate::accounting::TokenEstimator,
    pub(crate) usage: crate::usage_count::Counter,
}

impl Accounting {
    pub(crate) fn new(adapter: Adapter) -> Arc<Self> {
        Arc::new(Self {
            estimator: crate::accounting::TokenEstimator::new(adapter),
            usage: crate::usage_count::Counter::default(),
        })
    }
}

/// Exact model selection and its provider, ready for a harness execution scope.
pub struct ResolvedModel {
    pub(crate) files: crate::files::Files,
    /// Calibration and observed usage share one original selection owner.
    pub(crate) accounting: Arc<Accounting>,
    /// Immutable harness model identity.
    pub(crate) model: Model,
    /// Sealed settings derived from the parsed profile, never a root option map.
    pub(crate) profile: Option<acyclic_harness_core::model::ModelProfileBinding>,
    pub(crate) config: Arc<ProviderSpec>,
    pub(crate) capacity: ModelContextCapacity,
    pub(crate) tool_profile: Option<acyclic_harness_core::model::NativeToolProfile>,
    /// Retained from the same catalog member as capacity at original selection.
    pub(crate) thinking_levels: bool,
    pub(crate) key: Option<Arc<str>>,
    pub(crate) credential_scope: Option<[u8; 32]>,
    /// The platform transport with the endpoint's optional caller-selected deadline.
    pub(crate) transport: Arc<dyn HttpTransport>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) codex_login: Option<Arc<crate::CodexLogin>>,
}

impl ResolvedModel {
    // All derived selections retain these original owners. Only content
    // rebinding changes the reader, and only profile admission changes identity.
    fn derived(
        &self,
        model: Model,
        profile: Option<acyclic_harness_core::model::ModelProfileBinding>,
        files: crate::files::Files,
    ) -> Self {
        Self {
            files,
            accounting: Arc::clone(&self.accounting),
            model,
            profile,
            config: self.config.clone(),
            capacity: self.capacity,
            tool_profile: self.tool_profile,
            thinking_levels: self.thinking_levels,
            key: self.key.clone(),
            credential_scope: self.credential_scope,
            transport: self.transport.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            codex_login: self.codex_login.clone(),
        }
    }

    /// The original selected catalog member's tool family, including under
    /// alloy. Profile admission consumes this value without a naming heuristic.
    pub fn native_tool_profile(&self) -> Option<acyclic_harness_core::model::NativeToolProfile> {
        self.tool_profile
    }
    /// Joins the original registry selection to its admitted epoch profile.
    /// Credentials, endpoint and token counting retain their original owners.
    #[cfg(any(not(target_arch = "wasm32"), feature = "browser"))]
    pub fn with_profile(
        mut self,
        profile: &acyclic_harness_core::profile::AgentProfile<'_>,
    ) -> Result<Self> {
        let binding = self.admit_binding(profile)?;
        self.model.revision = self.profile_revision(&binding);
        self.profile = Some(binding);
        Ok(self)
    }

    #[cfg(any(not(target_arch = "wasm32"), feature = "browser"))]
    fn admit_binding(
        &self,
        profile: &acyclic_harness_core::profile::AgentProfile<'_>,
    ) -> Result<acyclic_harness_core::model::ModelProfileBinding> {
        if self.profile.is_some() {
            return Err(invalid(
                "a resolved model profile is immutable; resolve the next epoch",
            ));
        }
        let binding = profile.bind_model(&self.model, self.capacity)?;
        crate::projection::validate_profile(self.config.adapter, &binding)?;
        if self.config.adapter == crate::Adapter::Gemini
            && matches!(
                binding.reasoning(),
                acyclic_model_render::Reasoning::Level(_)
            )
            && !self.thinking_levels
        {
            return Err(invalid(
                "Gemini profile admission failed: thinking levels are not advertised by the selected model; recovery: select its provider-default reasoning profile",
            ));
        }
        Ok(binding)
    }

    #[cfg(any(not(target_arch = "wasm32"), feature = "browser"))]
    fn profile_revision(
        &self,
        binding: &acyclic_harness_core::model::ModelProfileBinding,
    ) -> String {
        let mut revision = blake3::Hasher::new();
        revision.update(b"acyclic.model-profile-binding.v1\0");
        revision.update(&(self.model.revision.len() as u64).to_be_bytes());
        revision.update(self.model.revision.as_bytes());
        revision.update(&binding.digest());
        let digest = revision.finalize().to_hex();
        let mut key = String::with_capacity("profile-v1:".len() + digest.len());
        key.push_str("profile-v1:");
        key.push_str(digest.as_str());
        key
    }

    /// Pinned profile and prefix identity for the original epoch publication.
    pub fn profile_binding(&self) -> Option<&acyclic_harness_core::model::ModelProfileBinding> {
        self.profile.as_ref()
    }

    /// Borrows the immutable identity used to bind this provider into the harness.
    pub fn model(&self) -> &Model {
        &self.model
    }
    pub(crate) fn reasoning_origin(
        &self,
        key: Option<&str>,
        account: Option<&str>,
    ) -> acyclic_harness_core::model::ReasoningOrigin {
        let family = crate::stream::family(self.config.adapter);
        // Native continuity belongs to whoever authenticates the request. An
        // anonymous route sends no credential of ours: the endpoint and its
        // configured headers (a gateway or proxy that injects credentials)
        // are that owner, so the route itself scopes the bytes it signed.
        // (ownership marker, credential bytes, whether the route headers
        // identify the owner)
        let owner: Option<(&[u8], &[u8], bool)> = match (
            self.credential_scope.as_ref(),
            account.or(key).filter(|value| !value.is_empty()),
        ) {
            (Some(scope), _) => Some((&b"protected-account\0"[..], &scope[..], false)),
            (None, Some(credential)) => Some((&b""[..], credential.as_bytes(), false)),
            (None, None) if self.config.authentication == crate::Authentication::Optional => {
                Some((&b"anonymous-route\0"[..], &b""[..], true))
            }
            (None, None) => None,
        };
        let credential_scope = owner.map(|(kind, credential, route)| {
            let mut scope = blake3::Hasher::new();
            scope.update(b"harness-reasoning-scope-v1\0");
            scope.update(kind);
            let mut field = |value: &[u8]| {
                scope.update(&(value.len() as u64).to_be_bytes());
                scope.update(value);
            };
            field(family.as_bytes());
            field(self.config.base_url.as_bytes());
            field(credential);
            if route {
                for (name, value) in &self.config.headers {
                    field(name.as_bytes());
                    field(value.as_bytes());
                }
            }
            *scope.finalize().as_bytes()
        });
        acyclic_harness_core::model::ReasoningOrigin {
            provider: family.into(),
            model: self.model.name.clone(),
            credential_scope,
        }
    }

    pub(crate) fn observe(
        &self,
        request: &PreparedModelRequest,
        input_bytes: usize,
        metadata: &serde_json::Value,
    ) -> Result<()> {
        self.accounting.estimator.observe(input_bytes, metadata);
        self.accounting
            .usage
            .observe(self.config.adapter, request, metadata)
    }

    pub(crate) fn admitted_capacity(&self) -> ModelContextCapacity {
        self.profile
            .as_ref()
            .map_or(self.capacity, |profile| profile.capacity())
    }
    pub(crate) fn validate(&self, model: &Model) -> Result<()> {
        if model != &self.model {
            return Err(invalid("request model differs from resolved selection"));
        }
        Ok(())
    }
}

impl ModelProvider for ResolvedModel {
    #[cfg(any(not(target_arch = "wasm32"), feature = "browser"))]
    fn admit_profile(
        &self,
        model: &Model,
        profile: &acyclic_harness_core::profile::AgentProfile<'_>,
    ) -> Result<(Model, Arc<dyn ModelProvider>)> {
        self.validate(model)?;
        let binding = self.admit_binding(profile)?;
        let model = Model {
            provider: self.model.provider.clone(),
            name: self.model.name.clone(),
            revision: self.profile_revision(&binding),
        };
        let admitted = self.derived(model, Some(binding), self.files.clone());
        Ok((admitted.model.clone(), Arc::new(admitted)))
    }

    fn bind_content(
        &self,
        reader: Arc<dyn acyclic_harness_core::conversation::ContentResidencyVerifier>,
        maximum_bytes: u64,
    ) -> Option<Arc<dyn ModelProvider>> {
        Some(Arc::new(self.derived(
            self.model.clone(),
            self.profile,
            crate::files::Files {
                reader: Some(reader),
                maximum_bytes: maximum_bytes.min(self.files.maximum_bytes),
            },
        )))
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
            return Err(acyclic_harness_core::Error::NativeBoundary {
                code: acyclic_harness_core::wire::ErrorCode::Invalid,
                reason: "request model differs from resolved selection",
            });
        }
        Ok(self.admitted_capacity())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn count_tokens_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        ceiling: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_harness_core::NativeFutureBundle<
            'a,
            (ModelTokenCount, acyclic_fs::NativeAllocation),
        >,
    > {
        self.context_capacity_with_native_work(&request.request().model, work)?;
        acyclic_harness_core::NativeFutureBundle::new(work, || async move {
            let cancelled = work.cancellation_wait()?;
            let exchange = crate::counting::count(self, request, ceiling, Some(work));
            let cancelled = core::pin::pin!(cancelled);
            let exchange = core::pin::pin!(exchange);
            match futures::future::select(cancelled, exchange).await {
                futures::future::Either::Left(_) => {
                    Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into())
                }
                futures::future::Either::Right((result, _)) => {
                    let (count, custody) = result?;
                    Ok((
                        count,
                        custody.ok_or(acyclic_harness_core::Error::NativeBoundary {
                            code: acyclic_harness_core::wire::ErrorCode::Invalid,
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
        acyclic_harness_core::NativeFutureBundle<
            'a,
            acyclic_harness_core::model::NativeModelStream<'a>,
        >,
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
        acyclic_harness_core::NativeFutureBundle<
            'a,
            (Option<Vec<ModelEvent>>, acyclic_fs::NativeAllocation),
        >,
    > {
        acyclic_harness_core::NativeFutureBundle::new(work, || async move {
            Ok((None, work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?))
        })
    }

    fn model_profile(
        &self,
        model: &Model,
    ) -> Result<Option<acyclic_harness_core::model::ModelProfileBinding>> {
        self.validate(model)?;
        Ok(self.profile)
    }

    fn native_tool_profile(
        &self,
        model: &Model,
    ) -> Result<Option<acyclic_harness_core::model::NativeToolProfile>> {
        self.validate(model)?;
        Ok(self.tool_profile)
    }

    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        self.validate(model)?;
        Ok(self.admitted_capacity())
    }

    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        ceiling: u64,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
            crate::counting::count(self, request, ceiling, None)
                .await
                .map(|(count, _)| count)
        })
    }

    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> acyclic_harness_core::model::ModelStream<'a> {
        acyclic_harness_core::model::ModelStream::new(crate::dispatch::generate(
            self, request, dispatch,
        ))
        .with_delivery(acyclic_harness_core::model::ModelDelivery::Live)
    }
}
