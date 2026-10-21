use crate::{HttpRequest, ResolvedModel, counting, dispatch, invalid, projection};
use acyclic_harness_core::{Result, context::ModelTokenCount, model::PreparedModelRequest};
use bytes::Bytes;

/// Request-local immutable HTTP inputs. Canonical admission, file verification
/// and provider projection happen once, before counting or generation.
pub(crate) struct PreparedInput {
    pub request: PreparedModelRequest,
    pub generation: Bytes,
    count: Bytes,
}

impl PreparedInput {
    pub async fn new(provider: &ResolvedModel, request: PreparedModelRequest) -> Result<Self> {
        provider.validate(&request.request().model)?;
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
        let generation = serde_json::to_vec(&body)
            .map_err(|_| invalid("model request encoding failed"))?
            .into();
        let count = serde_json::to_vec(&counting::wire::body(
            &body,
            &provider.model.name,
            provider.config.adapter,
        ))
        .map_err(|_| invalid("token count encoding failed"))?
        .into();
        drop(body);
        Ok(Self {
            request,
            generation,
            count,
        })
    }

    pub async fn count(&self, provider: &ResolvedModel) -> Result<ModelTokenCount> {
        let spec = provider
            .config
            .token_counter
            .as_ref()
            .ok_or_else(|| invalid("missing token counter"))?;
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
        counting::send_count(
            provider,
            exchange,
            projection::plan(provider, &self.request, 0)?,
            None,
            None,
        )
        .await
        .map(|(count, _)| count)
    }
}
