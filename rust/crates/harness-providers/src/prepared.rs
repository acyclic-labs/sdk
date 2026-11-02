use crate::{HttpRequest, ResolvedModel, counting, dispatch, projection};
use acyclic_harness_core::{Result, context::ModelTokenCount, model::PreparedModelRequest};
use bytes::Bytes;

/// Request-local immutable HTTP inputs. Canonical admission, file verification
/// and provider projection happen once, before counting or generation. Planning
/// weights stay with this preparation; each count still obtains a verified total.
pub(crate) struct PreparedInput {
    pub request: PreparedModelRequest,
    pub generation: Bytes,
    pub cache: Option<acyclic_harness_core::executor::ModelCacheObservation>,
    count: Bytes,
    plan: ModelTokenCount,
}

impl PreparedInput {
    pub async fn new(provider: &ResolvedModel, request: PreparedModelRequest) -> Result<Self> {
        provider.validate(&request.request().model)?;
        let spec = counting::specification(&provider.config, None)?;
        let plan = projection::plan(provider, &request, 0)?;
        let names = crate::tool_names::Names::new(request.request());
        let auth = dispatch::authenticate(
            provider,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )
        .await?;
        let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
        let mut files = crate::files::resolve(&provider.files, request.request(), None).await?;
        let body =
            projection::body_named(provider, request.request(), &names, &origin, &mut files)?;
        let encoded = projection::cache::encode(&body, provider, &origin)?;
        let count = counting::wire::body(body, &provider.model.name, spec.adapter)?.into();
        Ok(Self {
            request,
            generation: encoded.bytes.into(),
            cache: encoded.cache,
            count,
            plan,
        })
    }

    pub async fn count(&self, provider: &ResolvedModel) -> Result<ModelTokenCount> {
        provider.validate(&self.request.request().model)?;
        let spec = counting::specification(&provider.config, None)?;
        let auth = dispatch::authenticate(
            provider,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )
        .await?;
        let exchange = HttpRequest {
            url: counting::wire::endpoint(&provider.config.base_url, &provider.model.name, spec)?,
            headers: dispatch::headers(provider, auth.0.as_deref(), auth.1.as_deref(), false),
            body: self.count.clone(),
        };
        counting::send_count(provider, exchange, self.plan.clone(), None, None)
            .await
            .map(|(count, _)| count)
    }
}
