use crate::{HttpRequest, ResolvedModel, counting, dispatch, projection};
use acyclic_harness_core::{Result, context::ModelTokenCount, model::PreparedModelRequest};
use bytes::Bytes;
use futures::StreamExt as _;
use std::sync::Arc;

/// Request-local immutable HTTP inputs. Canonical admission, file verification
/// and provider projection happen once, bound to their original selection.
pub(crate) struct PreparedInput {
    provider: Arc<ResolvedModel>,
    pub request: PreparedModelRequest,
    pub generation: Bytes,
    cache: Option<acyclic_harness_core::executor::ModelCacheObservation>,
    count: Bytes,
}

impl PreparedInput {
    pub async fn new(provider: Arc<ResolvedModel>, request: PreparedModelRequest) -> Result<Self> {
        provider.validate(&request.request().model)?;
        let spec = counting::specification(&provider.config, None)?;
        let names = crate::tool_names::Names::new(request.request());
        let auth = dispatch::authenticate(
            &provider,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )
        .await?;
        let origin = provider.reasoning_origin(auth.0.as_deref(), auth.1.as_deref());
        let mut files = crate::files::resolve(&provider.files, &request, None).await?;
        let body =
            projection::body_named(&provider, request.request(), &names, &origin, &mut files)?;
        let encoded = projection::cache::encode(&body, &provider, &origin)?;
        let count = counting::wire::body(body, &provider.model.name, spec.adapter)?.into();
        drop(auth);
        drop(names);
        Ok(Self {
            provider,
            request,
            generation: encoded.bytes.into(),
            cache: encoded.cache,
            count,
        })
    }

    pub async fn count(&self) -> Result<ModelTokenCount> {
        let provider = &self.provider;
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

    /// Moves the caller's existing Arc or Rc into the stream without another
    /// erased frame or a separately selectable provider.
    pub fn generate(
        prepared: impl AsRef<Self> + acyclic_stream::ProviderTask,
        dispatch: acyclic_harness_core::model::ModelDispatch,
    ) -> impl futures::Stream<Item = Result<acyclic_harness_core::model::ModelEvent>>
    + acyclic_stream::ProviderTask {
        async_stream::try_stream! {
            let input = prepared.as_ref();
            let auth = dispatch::authenticate(
                &input.provider,
                #[cfg(not(target_arch = "wasm32"))]
                None,
            ).await?;
            let names = crate::tool_names::Names::new(input.request.request());
            let events = dispatch::generate_encoded(&input.provider, &input.request,
                dispatch, input.generation.clone(), input.cache, auth, names);
            futures::pin_mut!(events);
            while let Some(event) = events.next().await { yield event?; }
        }
    }
}
