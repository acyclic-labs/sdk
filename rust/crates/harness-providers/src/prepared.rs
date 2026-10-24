use crate::{ResolvedModel, counting, dispatch};
use acyclic_harness_core::{Result, context::ModelTokenCount, model::PreparedModelRequest};
use bytes::Bytes;

/// Request-local immutable HTTP inputs. Canonical admission, file verification
/// and provider projection happen once, before counting or generation.
pub(crate) struct PreparedInput {
    pub request: PreparedModelRequest,
    pub generation: Bytes,
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
        let bodies = crate::encoded::bodies(provider, &request, &names, &origin, None).await?;
        let generation = bodies.generation;
        Ok(Self {
            request,
            generation,
        })
    }

    pub async fn count(&self, provider: &ResolvedModel) -> Result<ModelTokenCount> {
        counting::count(provider, &self.request, None)
            .await
            .map(|(count, _)| count)
    }
}
